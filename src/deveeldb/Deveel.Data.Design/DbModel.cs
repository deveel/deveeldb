using System;

namespace Deveel.Data.Design {
	public sealed class DbModel {
		internal DbModel(DbModelBuilder modelBuilder) {
			ModelBuilder = modelBuilder;
		}

		internal DbModelBuilder ModelBuilder { get; private set; }

		public DbCompiledModel Compile() {
			var clonedBuilder = ModelBuilder.Clone();

			return new DbCompiledModel(clonedBuilder);
		}
	}
}
