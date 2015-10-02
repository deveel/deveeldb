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

namespace Deveel.Data.Store.Journaled {
	/// <summary>
	/// Provides a default implementation of a <see cref="IJournaledResource"/>.
	/// </summary>
	/// <remarks>
	/// Derived classes of this will be provided with unique identifiers
	/// of the resource within a system and a reference to the handler of
	/// data within the system.
	/// </remarks>
	public abstract class JournaledResource : IJournaledResource {
		/// <summary>
		/// Initializes a new instance of the <see cref="JournaledResource"/> class having a
		/// unique given name, unique identifier and an object that provides access to
		/// the backing data of the resource.
		/// </summary>
		/// <param name="name">The name.</param>
		/// <param name="id">The identifier.</param>
		/// <param name="storeData">The store data.</param>
		/// <exception cref="System.ArgumentNullException">
		/// storeData
		/// or
		/// name
		/// </exception>
		protected JournaledResource(string name, long id, IStoreData storeData) {
			if (storeData == null)
				throw new ArgumentNullException("storeData");
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			Name = name;
			Id = id;
			StoreData = storeData;
		}

		~JournaledResource() {
			Dispose(false);
		}

		/// <summary>
		/// Gets the reference to the object used to access store data for
		/// this resource within the system.
		/// </summary>
		protected IStoreData StoreData { get; private set; }

		/// <summary>
		/// Gets the unique name of the resource within the underlying system.
		/// </summary>
		public string Name { get; private set; }

		public abstract int PageSize { get; }

		public long Id { get; private set; }

		public bool IsReadOnly { get; private set; }

		public abstract bool Exists { get; }

		public abstract void Read(long pageNumber, byte[] buffer, int offset);

		public abstract void Write(long pageNumber, byte[] buffer, int offset, int length);

		public virtual long Size {
			get { return StoreData.Length; }
		}

		public virtual void SetSize(long value) {
			StoreData.SetLength(value);
		}

		/// <summary>
		/// Opens the resource for usage.
		/// </summary>
		/// <seealso cref="Open(bool)"/>
		protected abstract void OpenResource();

		public void Open(bool readOnly) {
			IsReadOnly = readOnly;
			OpenResource();
		}

		public abstract void Close();

		public abstract void Delete();

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			StoreData = null;
		}
	}
}
