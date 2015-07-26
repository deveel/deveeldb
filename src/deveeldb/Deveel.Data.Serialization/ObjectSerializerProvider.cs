using System;

using DryIoc;

namespace Deveel.Data.Serialization {
	public abstract class ObjectSerializerProvider : IObjectSerializerResolver, IDisposable {
		private Container container;

		public ObjectSerializerProvider() {
			container = new Container();
			CallInit();
		}

		~ObjectSerializerProvider() {
			Dispose(false);
		}

		private void CallInit() {
			Init();
		}

		protected abstract void Init();

		protected void Register<TObject, TSerializer>()
			where TSerializer : class, IObjectSerializer, new() {
			var typeName = typeof (TObject).FullName;
			container.Register<IObjectSerializer, TSerializer>(serviceKey:typeName);
		}

		public IObjectSerializer ResolveSerializer(Type objectType) {
			if (objectType == null)
				throw new ArgumentNullException("objectType");

			var typeName = objectType.FullName;
			return container.Resolve<IObjectSerializer>(typeName, IfUnresolved.ReturnDefault);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (container != null)
					container.Dispose();
			}

			container = null;
		}
	}
}
