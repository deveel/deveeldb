using System;

namespace Deveel.Data.Design.Configuration {
	public sealed class ForeignKeyConfiguration : CascableAssociationConfiguration {
		public ForeignKeyConfiguration(AssociationModelConfiguration configuration)
			: base(configuration) {
		}
	}
}
