using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Design.Conventions;

namespace Deveel.Data.Design {
	public sealed class DbModelBuilder : ICloneable {
		private ModelConfiguration configuration;

		private static readonly IConvention[] DefaultConventions;

		internal DbModelBuilder()
			: this(new ModelConfiguration(), DefaultConventions) { 
		}

		private DbModelBuilder(ModelConfiguration configuration, IEnumerable<IConvention> conventions)
			: this(configuration, new ConventionRegistry(conventions)) { 
		}

		private DbModelBuilder(ModelConfiguration configuration, ConventionRegistry conventions) {
			this.configuration = configuration;
			Configurations = new TypeConfigurationRegistry(configuration);
			Conventions = conventions;
		}

		static DbModelBuilder() {
			DefaultConventions = new IConvention[] {
				new TableNameAttributeConvention(),
				new IgnoredMemberConvention(), 
				new ColumnAttributeConvention(), 
				new PluralizeTableNameConvention(), 
			};
		}

		public TypeConfigurationRegistry Configurations { get; private set; }

		public ConventionRegistry Conventions { get; private set; }

		public TypeConfiguration<TType> Type<TType>() where TType : class {
			return Configurations.GetOrAdd<TType>();
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public DbModelBuilder Clone() {
			return new DbModelBuilder(configuration.Clone(), Conventions.Clone());
		}

		public DbModel Build() {
			return new DbModel(this);
		}
	}
}
