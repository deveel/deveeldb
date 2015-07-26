using System;

using Deveel.Data.DbSystem;

using DryIoc;

namespace Deveel.Data.Sql.Statements {
	static class PreparedStaementSerializers {
		static PreparedStaementSerializers() {
			Resolver = new SerializerResolver();
		}

		public static IObjectSerializerResolver Resolver { get; private set; }

		#region SerializerResolver

		class SerializerResolver : IObjectSerializerResolver {
			private readonly Container container;

			public SerializerResolver() {
				container = new Container();
				Init();
			}

			private void Init() {
				Register<CreateTableStatement.Prepared, CreateTableStatement.Prepared.Serializer>();
			}

			private void Register<TStatement, TSerializer>()
				where TStatement : SqlPreparedStatement
				where TSerializer : SqlPreparedStatementSerializer<TStatement> {
				var typeName = typeof (TStatement).AssemblyQualifiedName;
				
				container.Register<IObjectSerializer, TSerializer>(serviceKey:typeName);
			} 

			public IObjectSerializer ResolveSerializer(Type objectType) {
				var typeName = objectType.AssemblyQualifiedName;

				return container.Resolve<IObjectSerializer>(typeName);
			}
		}

		#endregion
	}
}
