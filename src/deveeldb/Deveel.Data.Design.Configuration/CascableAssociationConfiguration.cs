using System;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design.Configuration {
	public class CascableAssociationConfiguration {
		private readonly AssociationModelConfiguration configuration;

		internal CascableAssociationConfiguration(AssociationModelConfiguration configuration) {
			this.configuration = configuration;
		}

		public void CascadeOnDelete(bool value = true) {
			if (value)
				configuration.DeleteAction = ForeignKeyAction.Cascade;
		}
	}
}
