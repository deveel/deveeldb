using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Deveel.Data.Mapping;

using IQToolkit.Data;
using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	class DeveelDbMapping : BasicMapping {
		public DeveelDbMapping(MappingModel model) {
			Model = model;
		}

		public MappingModel Model { get; private set; }

		public override string GetColumnName(MappingEntity entity, MemberInfo member) {
			var dbMapping = (DbMappingEntity) entity;
			var memberMapping = dbMapping.TypeMapping.GetMember(member.Name);
			if (memberMapping == null)
				return null;

			return memberMapping.ColumnName;
		}

		public override MappingEntity GetEntity(Type elementType, string tableId) {
			var typeMapping = Model.GetMapping(elementType);
			if (typeMapping == null)
				throw new QueryException(String.Format("The type '{0}' is not mapped by the model", elementType));

			return new DbMappingEntity(tableId, typeMapping);
		}

		public override bool IsPrimaryKey(MappingEntity entity, MemberInfo member) {
			var dbMapping = (DbMappingEntity) entity;
			return dbMapping.TypeMapping.IsPrimaryKey(member.Name) ||
			       dbMapping.TypeMapping.IsUniqueKey(member.Name);
		}

		public override string GetTableName(MappingEntity entity) {
			var dbMapping = (DbMappingEntity)entity;
			return dbMapping.TypeMapping.TableName;
		}

		public override string GetTableId(Type type) {
			return type.FullName;
		}

		public override bool IsGenerated(MappingEntity entity, MemberInfo member) {
			var dbMapping = (DbMappingEntity)entity;
			return dbMapping.TypeMapping.IsUniqueKey(member.Name);
		}

		public override bool IsColumn(MappingEntity entity, MemberInfo member) {
			var dbMapping = (DbMappingEntity)entity;
			return dbMapping.TypeMapping.IsMemberMapped(member.Name) &&
			       dbMapping.TypeMapping.IsColumn(member.Name);
		}

		public override bool IsAssociationRelationship(MappingEntity entity, MemberInfo member) {
			return base.IsAssociationRelationship(entity, member);
		}

		public override IEnumerable<MemberInfo> GetAssociationKeyMembers(MappingEntity entity, MemberInfo member) {
			return base.GetAssociationKeyMembers(entity, member);
		}

		public override IEnumerable<MemberInfo> GetAssociationRelatedKeyMembers(MappingEntity entity, MemberInfo member) {
			return base.GetAssociationRelatedKeyMembers(entity, member);
		}

		public override bool IsRelationshipSource(MappingEntity entity, MemberInfo member) {
			// TODO:
			return false;
		}

		public override bool IsRelationshipTarget(MappingEntity entity, MemberInfo member) {
			// TODO:
			return false;
		}

		public override string GetColumnDbType(MappingEntity entity, MemberInfo member) {
			var dbMapping = (DbMappingEntity)entity;
			var mapppedMember = dbMapping.TypeMapping.GetMember(member.Name);
			if (mapppedMember == null)
				return null;

			// TODO: Check if here we should provide the full version of the type of just the name
			return mapppedMember.ColumnType.TypeCode.ToString();
		}

		#region DbMappingEntity

		class DbMappingEntity : MappingEntity {
			public DbMappingEntity(string tableId, TypeMapping typeMapping) {
				this.tableId = tableId;
				this.TypeMapping = typeMapping;
			}

			private readonly string tableId;

			public TypeMapping TypeMapping { get; }

			public override string TableId {
				get { return tableId; }
			}

			public override Type ElementType {
				get { return TypeMapping.Type; }
			}

			public override Type EntityType {
				get { return TypeMapping.Type; }
			}
		}

		#endregion
	}
}
