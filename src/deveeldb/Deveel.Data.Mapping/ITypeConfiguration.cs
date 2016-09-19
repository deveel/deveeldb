using System;
using System.Linq.Expressions;

namespace Deveel.Data.Mapping {
	public interface ITypeConfiguration<TType> {
		ITypeConfiguration<TType> HasTableName(string value);

		IMemberConfiguration Member<TMember>(Expression<Func<TType, TMember>> selector);
	}
}
