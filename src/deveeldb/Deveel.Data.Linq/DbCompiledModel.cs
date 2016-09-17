using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using IQToolkit.Data.Common;

namespace Deveel.Data.Linq {
	public sealed class DbCompiledModel {
		private Dictionary<Type, DbTypeModel> typeModels;

		internal DbCompiledModel(Type contextType, IEnumerable<DbTypeModel> models) {
			ContextType = contextType;
			typeModels = models.ToDictionary(x => x.Type, y => y);
		}

		private Type ContextType { get; set; }

		public DbTypeModel GetTypeModel<T>() where T : class {
			return GetTypeModel(typeof(T));
		}

		public DbTypeModel GetTypeModel(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");

			DbTypeModel model;
			if (!typeModels.TryGetValue(type, out model))
				return null;

			return model;
		}

		public DbAssociationModel GetAssociationModel(Type sourceType, MemberInfo sourceMember) {
			throw new NotImplementedException();
		}

		public bool IsMapped(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");

			return typeModels.ContainsKey(type);
		}

		internal QueryMapping CreateMapping() {
			return new DbModelMapping(ContextType, this);
		}
	}
}
