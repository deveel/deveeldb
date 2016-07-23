using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;

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

			var entity = new ModelMappingEntity(elementType, tableId, entityType);

			var memberNames = typeModel.MemberNames;
			foreach (var memberName in memberNames) {
				ModelMappingEntityMember memberMapping;

				if (typeModel.IsMember(memberName)) {
					var columnModel = typeModel.GetColumn(memberName);
					memberMapping = new ModelMappingEntityMember {
						ColumnName = columnModel.ColumnName,
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
			throw new NotImplementedException();
		}

		public override bool IsRelationshipTarget(MappingEntity entity, MemberInfo member) {
			throw new NotImplementedException();
		}

		public override bool IsNestedEntity(MappingEntity entity, MemberInfo member) {
			throw new NotImplementedException();
		}

		public override IList<MappingTable> GetTables(MappingEntity entity) {
			throw new NotImplementedException();
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

			public ModelMappingEntity(Type elementType, string tableId, Type entityType) {
				this.elementType = elementType;
				this.tableId = tableId;
				this.entityType = entityType;

				members = new Dictionary<string, ModelMappingEntityMember>();
			}

			public override string TableId {
				get { return tableId; }
			}

			public override Type ElementType {
				get { return elementType; }
			}

			public override Type EntityType {
				get { return entityType; }
			}

			public ModelMappingEntityMember GetMember(string memberName) {
				ModelMappingEntityMember member;
				if (!members.TryGetValue(memberName, out member))
					return null;

				return member;
			}

			public void AddMember(ModelMappingEntityMember memberMapping) {
				throw new NotImplementedException();
			}
		}
		#endregion

		#region ModelMappingEntityMember

		class ModelMappingEntityMember {
			public string MemberName { get; set; }

			public string ColumnName { get; set; }

			public string AliasName { get; set; }

			public bool IsKey { get; set; }

			public bool IsAssociation { get; set; }

			public bool IsForeignKey { get; set; }

			public string RelatedId { get; set; }

			public Type RelatedEntityType { get; set; }

			public string KeyMembers { get; set; }

			public string RelatedKeyMembers { get; set; }
		}

		#endregion
	}
}