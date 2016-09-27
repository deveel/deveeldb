using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Deveel.Data.Design.Configuration {
	public sealed class RequiredAssociationConfiguration<TType>
		where TType : class {
		private readonly AssociationModelConfiguration configuration;

		internal RequiredAssociationConfiguration(AssociationModelConfiguration configuration) {
			this.configuration = configuration;
		}

		public DependentAssociationConfiguration<TType> WithMany<TTarget>(
			Expression<Func<TTarget, ICollection<TType>>> selector) {
			var member = TypeUtil.FindMember(selector);
			var targetModel = configuration.TypeModel.Model.Type(typeof(TTarget));
			var targetMember = targetModel.GetMember(member.Name);

			configuration.TargetMember = targetMember;
			configuration.Cardinality = AssociationCardinality.OneToMany;

			return new DependentAssociationConfiguration<TType>(configuration);
		}

		public ForeignKeyConfiguration WithOptional<TTarget>(Expression<Func<TTarget, TType>> selector) {
			var member = TypeUtil.FindMember(selector);
			var targetModel = configuration.TypeModel.Model.Type(typeof(TTarget));
			var targetMember = targetModel.GetMember(member.Name);

			configuration.TargetMember = targetMember;
			configuration.Cardinality = AssociationCardinality.OneToOne;
			configuration.Function = AssociationFunction.Optional;

			return new ForeignKeyConfiguration(configuration);
		}

		public ForeignKeyConfiguration WithRequiredDependent<TTarget>(Expression<Func<TTarget, TType>> selector) {
			var member = TypeUtil.FindMember(selector);
			var targetModel = configuration.TypeModel.Model.Type(typeof(TTarget));
			var targetMember = targetModel.GetMember(member.Name);

			configuration.TargetMember = targetMember;
			configuration.Cardinality = AssociationCardinality.OneToOne;
			configuration.Function = AssociationFunction.Required;

			return new ForeignKeyConfiguration(configuration);
		}
	}
}
