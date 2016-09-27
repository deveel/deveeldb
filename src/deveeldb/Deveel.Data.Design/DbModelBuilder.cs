using System;
using System.Collections.Generic;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Design.Conventions;

namespace Deveel.Data.Design {
	public sealed class DbModelBuilder : ICloneable {
		internal ModelConfiguration ModelConfiguration { get; private set; }

		private static readonly IConvention[] DefaultConventions;

		internal DbModelBuilder()
			: this(new ModelConfiguration(), DefaultConventions) { 
		}

		private DbModelBuilder(ModelConfiguration configuration, IEnumerable<IConvention> conventions)
			: this(configuration, new ConventionRegistry(conventions)) { 
		}

		private DbModelBuilder(ModelConfiguration configuration, ConventionRegistry conventions) {
			ModelConfiguration = configuration;
			Configurations = new TypeConfigurationRegistry(configuration);
			Conventions = conventions;
		}

		static DbModelBuilder() {
			DefaultConventions = new IConvention[] {
				new IgnoredTypeAttributeConvention(),
				new TableNameAttributeConvention(),
				new TypeMemberDiscoveryConvention(), 
				new IgnoredPropertyAttributeConvention(), 
				new ColumnAttributeConvention(),
				new PrimaryKeyAttributeConvention(), 
				new UniqueAttributeConvention(),
				new DefaultAttributeConvention(),
				new GeneratedAttributeConvention(), 
				new PrimitiveTypeResolvingConvention(), 
				new PrimaryKeyDiscoveryConvention(), 
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
			return new DbModelBuilder(ModelConfiguration.Clone(), Conventions.Clone());
		}

		public DbModel Build() {
			return new DbModel(this);
		}
	}
}
