using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;

using Deveel.Data.Sql.Types;

using IQToolkit;
using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class DbModelMapping : AdvancedMapping {
		private ReaderWriterLock rwLock;
		private Dictionary<string, MappingEntity> entities;
		private Type contextType;

		public DbModelMapping(Type contextType, DbCompiledModel model) {
			Model = model;
			this.contextType = contextType;
			rwLock = new ReaderWriterLock();
			entities = new Dictionary<string, MappingEntity>();
		}

		public DbCompiledModel Model { get; private set; }

		public override MappingEntity GetEntity(MemberInfo contextMember) {
			var elementType = TypeHelper.GetElementType(TypeHelper.GetMemberType(contextMember));
			return GetEntity(elementType, contextMember.Name);
		}

		public override MappingEntity GetEntity(Type elementType, string tableId) {
			return GetEntity(elementType, tableId, elementType);
		}

		private MappingEntity GetEntity(Type elementType, string tableId, Type entityType) {
			MappingEntity entity;
			rwLock.AcquireReaderLock(Timeout.Infinite);
			if (!entities.TryGetValue(tableId, out entity)) {
				rwLock.ReleaseReaderLock();
				rwLock.AcquireWriterLock(Timeout.Infinite);
				if (!entities.TryGetValue(tableId, out entity)) {
					entity = CreateEntity(elementType, tableId, entityType);
					entities.Add(tableId, entity);
				}
				rwLock.ReleaseWriterLock();
			} else {
				rwLock.ReleaseReaderLock();
			}
			return entity;
		}

		private MappingEntity CreateEntity(Type elementType, string tableId, Type entityType) {
			if (tableId == null)
				tableId = GetTableId(elementType);

			var typeModel = Model.GetTypeModel(entityType);

			if (typeModel == null)
				return null;

			var entity = new ModelMappingEntity(elementType, tableId, entityType, typeModel.TableName);

			var memberNames = typeModel.MemberNames;
			foreach (var memberName in memberNames) {
				ModelMappingEntityMember memberMapping;

				if (typeModel.IsMember(memberName)) {
					var columnModel = typeModel.GetColumn(memberName);
					memberMapping = new ModelMappingEntityMember {
						ColumnName = columnModel.ColumnName,
						ColumnType = columnModel.ColumnType,
						MemberName = columnModel.Member.Name,
						IsKey = columnModel.IsKey
					};
				} else if (typeModel.IsAssociation(memberName)) {
					var association = typeModel.GetAssociation(memberName);
					memberMapping = new ModelMappingEntityMember {
						MemberName = association.Member.Name,
						IsAssociation = true,
						RelatedEntityType = association.DestinationType.Type,
						KeyMembers = association.SourceColumn.Member.Name,
						RelatedKeyMembers = association.DestinationColumn.Member.Name
					};
				} else {
					throw new InvalidOperationException();
				}

				entity.AddMember(memberMapping);
			}

			return entity;
		}

		public override string GetTableId(Type entityType) {
			if (contextType != null) {
				foreach (var mi in contextType.GetMembers(BindingFlags.Instance | BindingFlags.Public)) {
					FieldInfo fi = mi as FieldInfo;
					if (fi != null && TypeHelper.GetElementType(fi.FieldType) == entityType)
						return fi.Name;
					PropertyInfo pi = mi as PropertyInfo;
					if (pi != null && TypeHelper.GetElementType(pi.PropertyType) == entityType)
						return pi.Name;
				}
			}

			return entityType.Name;
		}

		public override bool IsRelationshipSource(MappingEntity entity, MemberInfo member) {
			var modelEntity = (ModelMappingEntity) entity;
			var entityMember = modelEntity.GetMember(member.Name);
			if (entityMember == null ||
				!entityMember.IsAssociation)
				return false;

			return entityMember.AssociationType != AssociationType.Dependant;
		}

		public override bool IsRelationshipTarget(MappingEntity entity, MemberInfo member) {
			var modelEntity = (ModelMappingEntity)entity;
			var entityMember = modelEntity.GetMember(member.Name);
			if (entityMember == null ||
				!entityMember.IsAssociation)
				return false;

			return entityMember.AssociationType == AssociationType.Dependant;
		}

		public override bool IsNestedEntity(MappingEntity entity, MemberInfo member) {
			// TODO: should support nested entities?
			return false;
		}

		public override IList<MappingTable> GetTables(MappingEntity entity) {
			return new[] { ((ModelMappingEntity) entity).Table };
		}

		public override string GetAlias(MappingTable table) {
			throw new NotImplementedException();
		}

		public override string GetAlias(MappingEntity entity, MemberInfo member) {
			throw new NotImplementedException();
		}

		public override string GetTableName(MappingTable table) {
			throw new NotImplementedException();
		}

		public override string GetTableName(MappingEntity entity) {
			var modelEntity = (ModelMappingEntity) entity;
			return modelEntity.TableName;
		}

		public override string GetColumnName(MappingEntity entity, MemberInfo member) {
			var modelEntity = (ModelMappingEntity) entity;
			var memberModel = modelEntity.GetMember(member.Name);
			if (memberModel == null || 
				String.IsNullOrEmpty(memberModel.ColumnName))
				return member.Name;

			return memberModel.ColumnName;
		}

		public override string GetColumnDbType(MappingEntity entity, MemberInfo member) {
			var modelEntity = (ModelMappingEntity)entity;
			var memberModel = modelEntity.GetMember(member.Name);
			if (memberModel == null)
				return GetSqlTypeName(GetMemberType(member));

			return base.GetColumnDbType(entity, member);
		}

		private static Type GetMemberType(MemberInfo member) {
			if (member is PropertyInfo)
				return ((PropertyInfo) member).PropertyType;
			if (member is FieldInfo)
				return ((FieldInfo) member).FieldType;

			throw new NotSupportedException();
		}

		private static string GetSqlTypeName(Type type) {
			if (type == typeof(bool))
				return "BOOLEAN";
			if (type == typeof(byte))
				return "TINYINT";
			if (type == typeof(short))
				return "SMALLINT";
			if (type == typeof(int))
				return "INTEGER";
			if (type == typeof(long))
				return "BIGINT";
			if (type == typeof(float))
				return "REAL";
			if (type == typeof(double))
				return "DOUBLE";

			if (type == typeof(string))
				return "VARCHAR";

			throw new NotSupportedException();
		}

		public override bool IsColumn(MappingEntity entity, MemberInfo member) {
			var modelEntity = (ModelMappingEntity)entity;
			var memberModel = modelEntity.GetMember(member.Name);
			if (memberModel == null)
				return false;

			return !memberModel.IsAssociation;
		}

		public override bool IsAssociationRelationship(MappingEntity entity, MemberInfo member) {
			var modelEntity = (ModelMappingEntity)entity;
			var memberModel = modelEntity.GetMember(member.Name);
			if (memberModel == null)
				return false;

			return memberModel.IsAssociation;
		}

		public override bool IsMapped(MappingEntity entity, MemberInfo member) {
			var modelEntity = (ModelMappingEntity)entity;
			var memberModel = modelEntity.GetMember(member.Name);
			return memberModel != null;
		}

		public override bool IsExtensionTable(MappingTable table) {
			throw new NotImplementedException();
		}

		public override string GetExtensionRelatedAlias(MappingTable table) {
			throw new NotImplementedException();
		}

		public override IEnumerable<string> GetExtensionKeyColumnNames(MappingTable table) {
			throw new NotImplementedException();
		}

		public override IEnumerable<MemberInfo> GetExtensionRelatedMembers(MappingTable table) {
			throw new NotImplementedException();
		}

		#region ModelMappingEntity

		class ModelMappingEntity : MappingEntity {
			private string tableId;
			private Type elementType;
			private Type entityType;

			private Dictionary<string, ModelMappingEntityMember> members;

			public ModelMappingEntity(Type elementType, string tableId, Type entityType, string tableName) {
				this.elementType = elementType;
				this.tableId = tableId;
				this.entityType = entityType;

				members = new Dictionary<string, ModelMappingEntityMember>();
				Table = new ModelMappingTable(this);

				TableName = tableName;
			}

			public string TableName { get; private set; }

			public override string TableId {
				get { return tableId; }
			}

			public override Type ElementType {
				get { return elementType; }
			}

			public override Type EntityType {
				get { return entityType; }
			}

			public MappingTable Table { get; private set; }

			public ModelMappingEntityMember GetMember(string memberName) {
				ModelMappingEntityMember member;
				if (!members.TryGetValue(memberName, out member))
					return null;

				return member;
			}

			public void AddMember(ModelMappingEntityMember memberMapping) {
				members.Add(memberMapping.MemberName, memberMapping);
			}

			#region ModelMappingTable

			class ModelMappingTable : MappingTable {
				public ModelMappingTable(ModelMappingEntity entity) {
					Entity = entity;
				}

				public ModelMappingEntity Entity { get; private set; }
			}

			#endregion
		}

		#endregion

		#region ModelMappingEntityMember

		class ModelMappingEntityMember {
			public string MemberName { get; set; }

			public string ColumnName { get; set; }

			public SqlType ColumnType { get; set; }

			public string AliasName { get; set; }

			public bool IsKey { get; set; }

			public bool IsAssociation { get; set; }

			public AssociationType AssociationType { get; set; }

			public string RelatedId { get; set; }

			public Type RelatedEntityType { get; set; }

			public string KeyMembers { get; set; }

			public string RelatedKeyMembers { get; set; }
		}

		#endregion
	}
}