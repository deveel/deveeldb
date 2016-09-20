using System;

using Deveel.Data.Design.Conventions;

namespace Deveel.Data.Design {
	public sealed class DbModelBuilder : ICloneable {
		internal DbModelBuilder() {
			Configurations = new TypeConfigurationRegistry();
			Conventions = new ConventionRegistry();

			AddDefaultConventions();
		}

		public TypeConfigurationRegistry Configurations { get; private set; }

		public ConventionRegistry Conventions { get; private set; }

		private void AddDefaultConventions() {
			Conventions.Add<TableNameAttributeConvention>();
			Conventions.Add<IgnoredMemberConvention>();
			Conventions.Add<ColumnAttributeConvention>();
			Conventions.Add<PluralizeTableNameConvention>();
		}

		public TypeConfiguration<TType> Type<TType>() where TType : class {
			return Configurations.GetOrAdd<TType>();
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public DbModelBuilder Clone() {
			return new DbModelBuilder {
				Configurations = Configurations.Clone(),
				Conventions = Conventions.Clone()
			};
		}

		public DbModel Build() {
			return new DbModel(this);
		}
	}
}
