using System;

namespace Deveel.Data.Design {
	public interface ICascableAssociationConfiguration {
		void CascadeOnDelete(bool value = true);
	}
}
