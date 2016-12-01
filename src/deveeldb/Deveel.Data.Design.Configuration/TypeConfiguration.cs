using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design.Configuration {
	public class TypeConfiguration<TType> where TType : class {
		public TypeConfiguration()
			: this(new ModelConfiguration()) {
		}

		internal TypeConfiguration(ModelConfiguration model) {
			TypeModel = model.Type(typeof(TType));
		}

		private TypeModelConfiguration TypeModel { get; set; }

		internal void AttachTo(ModelConfiguration model) {
			throw new NotImplementedException();
		}

		public TypeConfiguration<TType> HasTableName(string value) {
			TypeModel.TableName = value;
			return this;
		}

		public TypeConfiguration<TType> Check(SqlExpression expression) {
			if (expression == null)
				throw new ArgumentNullException("expression");

			var constraint = TypeModel.Constraint(ConstraintType.Check);
			constraint.CheckExpression = expression;

			return this;
		}

		private MemberModelConfiguration GetMemberModel<TMember>(Expression<Func<TType, TMember>> selector) {
			var memberInfo = TypeUtil.FindMember(selector);

			if (memberInfo == null)
				throw new InvalidOperationException();

			return TypeModel.GetMember(memberInfo.Name);
		}

		public MemberConfiguration<TMember> Member<TMember>(Expression<Func<TType, TMember>> selector) {
			var configuration = GetMemberModel(selector);
			return new MemberConfiguration<TMember>(configuration);
		}

		public RequiredAssociationConfiguration<TType> HasRequired<TMember>(Expression<Func<TType, TMember>> selector) {
			var member = TypeUtil.FindMember(selector);
			var config = TypeModel.Model.Associate(TypeModel.Type, member.Name, AssociationType.Source);

			return new RequiredAssociationConfiguration<TType>(config);
		}

		public ManyAssociationConfiguration<TType, TTarget> WithMany<TTarget>(
			Expression<Func<TType, ICollection<TTarget>>> selector) where TTarget : class {
			var member = TypeUtil.FindMember(selector);
			var config = TypeModel.Model.Associate(TypeModel.Type, member.Name, AssociationType.Destination);

			return new ManyAssociationConfiguration<TType, TTarget>(config);
		}
	}
}
