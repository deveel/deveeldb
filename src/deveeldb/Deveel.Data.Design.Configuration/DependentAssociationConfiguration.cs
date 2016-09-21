using System;
using System.Linq.Expressions;

namespace Deveel.Data.Design.Configuration {
	public sealed class DependentAssociationConfiguration<TType> : CascableAssociationConfiguration where TType : class {
		private AssociationModelConfiguration configuration;

		internal DependentAssociationConfiguration(AssociationModelConfiguration configuration)
			: base(configuration) {
			this.configuration = configuration;
		}

		public CascableAssociationConfiguration HasForeignKey<TKey>(Expression<Func<TType, TKey>> selector) {
			var member = TypeUtil.FindMember(selector);
			var memberModel = configuration.TypeModel.GetMember(member.Name);

			configuration.KeyMember = memberModel;
			return new CascableAssociationConfiguration(configuration);
		}
	}
}
