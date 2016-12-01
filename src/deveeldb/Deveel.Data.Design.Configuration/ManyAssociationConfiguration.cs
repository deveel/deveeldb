using System;
using System.Linq.Expressions;

namespace Deveel.Data.Design.Configuration {
	public sealed class ManyAssociationConfiguration<TType, TTarget> where TType : class where TTarget : class {
		private readonly AssociationModelConfiguration configuration;

		internal ManyAssociationConfiguration(AssociationModelConfiguration configuration) {
			this.configuration = configuration;
		}

		public DependentAssociationConfiguration<TType> WithRequired(Expression<Func<TTarget, TType>> selector) {
			throw new NotImplementedException();
		}
	}
}
