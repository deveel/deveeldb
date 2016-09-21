using System;
using System.Reflection;

namespace Deveel.Data.Design {
	public sealed class DbModel {
		internal DbModel(DbModelBuilder modelBuilder) {
			ModelBuilder = modelBuilder;
		}

		internal DbModelBuilder ModelBuilder { get; private set; }

		public DbCompiledModel Compile() {
			var builder = ModelBuilder.Clone();

			foreach (var convention in ModelBuilder.Conventions.SortedStructuralConventions()) {
				convention.Apply(this);
			}

			foreach (var convention in ModelBuilder.Conventions.SortedConfigurationConventions()) {
				foreach (var type in ModelBuilder.ModelConfiguration.Types) {
					convention.Apply(type, builder.ModelConfiguration);

					var members = type.GetMembers(BindingFlags.Instance | BindingFlags.Public);

					foreach (var member in members) {
						convention.Apply(member, builder.ModelConfiguration);
					}
				}

				builder = builder.Clone();
			}

			return new DbCompiledModel(builder);
		}
	}
}
