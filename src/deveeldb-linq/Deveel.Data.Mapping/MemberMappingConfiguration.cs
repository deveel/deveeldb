using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	public sealed class MemberMappingConfiguration<T> : IMemberMappingConfiguration where T : class {
		private readonly MemberInfo memberInfo;
		private readonly TypeMappingConfiguration<T> typeMapping; 

		internal MemberMappingConfiguration(TypeMappingConfiguration<T> typeMapping, MemberInfo memberInfo) {
			this.memberInfo = memberInfo;
			this.typeMapping = typeMapping;

			DiscoverAttributes();
		}

		private string ColumnName { get; set; }

		private SqlTypeCode? ColumnTypeCode { get; set; }

		private int? Size { get; set; }

		private int? Precision { get; set; }

		private ColumnConstraints ColumnConstraints { get; set; }

		private void DiscoverAttributes() {
			string columnName = null;
			ColumnConstraints constraints = new ColumnConstraints();
			SqlTypeCode? typeCode = null;
			int? size = null;
			int? scale = null;

			var attributes = memberInfo.GetCustomAttributes(false);
			foreach (var attribute in attributes) {
				if (attribute is ColumnNameAttribute) {
					var columnNameAttr = (ColumnNameAttribute) attribute;
					ColumnName = columnNameAttr.ColumnName;					
				} else if (attribute is ColumnAttribute) {
					var columnAttr = (ColumnAttribute) attribute;
					columnName = columnAttr.ColumnName;
					typeCode = columnAttr.SqlType;
					size = columnAttr.Size;
					scale = columnAttr.Scale;
				} else if (attribute is ColumnConstraintAttribute) {
					var constraintAttr = (ColumnConstraintAttribute) attribute;
					constraints |= constraintAttr.Constraints;
				}
			}

			ColumnName = columnName;
			ColumnTypeCode = typeCode;
			ColumnConstraints = constraints;
			Size = size;
			Precision = scale;
		}

		string IMemberMappingConfiguration.ColumnName {
			get { return ColumnName; }
		}

		SqlTypeCode? IMemberMappingConfiguration.ColumnType {
			get { return ColumnTypeCode; }
		}

		ColumnConstraints IMemberMappingConfiguration.ColumnConstraints {
			get { return ColumnConstraints; }
		}

		int? IMemberMappingConfiguration.Size {
			get { return Size; }
		}

		int? IMemberMappingConfiguration.Precision {
			get { return Precision; }
		}

		public MemberMappingConfiguration<T> HasName(string columnName) {
			ColumnName = columnName;
			return this;
		}

		public MemberMappingConfiguration<T> HasType(SqlTypeCode typeCode) {
			ColumnTypeCode = typeCode;
			return this;
		}

		public MemberMappingConfiguration<T> HasSize(int size) {
			Size = size;
			return this;
		}

		public MemberMappingConfiguration<T> IsUnique(bool flag = false) {
			if (flag) {
				ColumnConstraints |= ColumnConstraints.Unique;
			} else {
				ColumnConstraints &= ~(ColumnConstraints.Unique);
			}

			return this;
		}

		public MemberMappingConfiguration<T> IsNotNull(bool flag = false) {
			if (flag) {
				ColumnConstraints |= ColumnConstraints.NotNull;
			} else {
				ColumnConstraints &= ~(ColumnConstraints.NotNull);
			}

			return this;
		}

		public MemberMappingConfiguration<T> IsPrimaryKey(bool flag = false) {
			if (flag) {
				ColumnConstraints |= ColumnConstraints.PrimaryKey;
			} else {
				ColumnConstraints &= ~(ColumnConstraints.PrimaryKey);
			}

			return this;
		}

		public void Ignore() {
			typeMapping.Ignore(memberInfo.Name);
		}
	}
}
