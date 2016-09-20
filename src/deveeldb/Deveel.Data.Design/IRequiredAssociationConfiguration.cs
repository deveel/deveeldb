using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Deveel.Data.Design {
	public interface IRequiredAssociationConfiguration<TType, TTarget> : IAssociationConfiguration<TType, TTarget> 
		where TType : class
		where TTarget : class {
		IDependantAssociationConfiguration<TType, TTarget> WithMany(Expression<Func<TTarget, ICollection<TType>>> selector);

		IForeignKeyAssociationConfiguration<TType, TTarget> WithOptional(Expression<Func<TTarget, TType>> selector);
	}
}
