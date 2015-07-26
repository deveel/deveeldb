using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Serialization;

using DryIoc;

namespace Deveel.Data.Sql.Statements {
	static class PreparedStaementSerializers {
		static PreparedStaementSerializers() {
			Resolver = new SerializerResolver();
		}

		public static IObjectSerializerResolver Resolver { get; private set; }

		#region SerializerResolver

		class SerializerResolver : ObjectSerializerProvider {
			public SerializerResolver() {
				Init();
			}

			protected override void Init() {
				Register<CreateTableStatement.Prepared, CreateTableStatement.Prepared.Serializer>();
			}
		}

		#endregion
	}
}
