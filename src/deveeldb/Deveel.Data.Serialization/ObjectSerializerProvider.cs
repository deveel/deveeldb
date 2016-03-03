// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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
