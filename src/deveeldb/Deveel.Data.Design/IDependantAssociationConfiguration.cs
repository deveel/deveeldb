using System;
using System.Linq.Expressions;

namespace Deveel.Data.Design {
	public interface IDependantAssociationConfiguration<TType, TTarget> {
		ICascableAssociationConfiguration HasForeignKey<TKey>(Expression<Func<TType, TKey>> selector);
	}
}
