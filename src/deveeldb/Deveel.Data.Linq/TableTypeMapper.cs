using System;
using System.Collections.Generic;
using System.Reflection;

using Deveel.Data.Sql;

namespace Deveel.Data.Linq {
	class TableTypeMapper {
		private readonly Dictionary<string, int> columnOffsets;
		private readonly Dictionary<string, string> memberMapping; 
		private readonly List<string> columnNames; 

		public TableTypeMapper(Type elementType) {
			columnNames = new List<string>();
			columnOffsets = new Dictionary<string, int>();
			memberMapping = new Dictionary<string, string>();

			DiscoverType(elementType);

			ElementType = elementType;
		}

		public Type ElementType { get; private set; }

		private void DiscoverType(Type elementType) {
			var members = elementType.FindMembers(MemberTypes.Field | MemberTypes.Property,
				BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, FilterColumns, null);

			foreach (var member in members) {
				var columnAttr = (ColumnAttribute) Attribute.GetCustomAttribute(member, typeof (ColumnAttribute));
				var columnName = member.Name;
				if (!String.IsNullOrEmpty(columnAttr.ColumnName))
					columnName = columnAttr.ColumnName;

				columnNames.Add(columnName);
				memberMapping[member.Name] = columnName;
			}
		}

		private static bool FilterColumns(MemberInfo member, object criteria) {
			return Attribute.IsDefined(member, typeof (ColumnAttribute));
		}

		public void BuildMap(ITable table) {
			foreach (var columnName in columnNames) {
				var columnOffset = table.TableInfo.IndexOfColumn(columnName);
				if (columnOffset >= 0)
					columnOffsets[columnName] = columnOffset;
			}
		}

		public object Construct(ITable table, int rowOffset) {
			var obj = Activator.CreateInstance(ElementType, true);
			var members = ElementType.FindMembers(MemberTypes.Field | MemberTypes.Property,
				BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, FilterColumns, null);

			foreach (var member in members) {
				string columnName;
				if (!memberMapping.TryGetValue(member.Name, out columnName))
					continue;

				int offset;
				if (!columnOffsets.TryGetValue(columnName, out offset))
					continue;

				Type memberType;

				if (member is PropertyInfo) {
					memberType = ((PropertyInfo) member).PropertyType;
				} else {
					memberType = ((FieldInfo) member).FieldType;
				}

				var value = table.GetValue(rowOffset, offset);
				var finalValue = ToMemberValue(value, memberType);

				if (member is PropertyInfo) {
					((PropertyInfo)member).SetValue(obj, finalValue, null);
				} else {
					((FieldInfo)member).SetValue(obj, finalValue);
				}
			}

			return obj;
		}

		private object ToMemberValue(DataObject value, Type memberType) {
			var obj = value.Value;
			return value.Type.ConvertTo(obj, memberType);
		}
	}
}
