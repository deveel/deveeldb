// 
//  JournalledFileStore.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Threading;

namespace Deveel.Data.Store {
	/// <summary>
	/// An implementation of <see cref="AbstractStore"/> that persists 
	/// to an underlying data format via a robust journalling system 
	/// that supports check point and crash recovery.
	/// </summary>
	/// <remarks>
	/// This object is a bridge between the <b>Store API</b> and the 
	/// journalled behaviour defined in <see cref="LoggingBufferManager"/>, 
	/// <see cref="JournalledSystem"/> and the <see cref="IStoreDataAccessor"/> 
	/// implementations.
	/// <para>
	/// Access to the resources is abstracted via a <i>resource name</i>
	/// string. The <see cref="LoggingBufferManager"/> object converts the resource 
	/// name into a concrete object that accesses the actual data.
	/// </para>
	/// </remarks>
	public sealed class JournalledFileStore : AbstractStore {
		/// <summary>
		/// The name of the resource.
		/// </summary>
		private readonly String resource_name;

		/// <summary>
		/// The buffering strategy for accessing the data in an underlying file.
		/// </summary>
		private readonly LoggingBufferManager buffer_manager;

		/// <summary>
		/// The <see cref="IJournalledResource"/> object that's used to journal all 
		/// read/write operations to the above <i>store_accessor</i>.
		/// </summary>
		private readonly IJournalledResource store_resource;


		public JournalledFileStore(String resource_name,
								   LoggingBufferManager buffer_manager,
								   bool read_only)
			: base(read_only) {
			this.resource_name = resource_name;
			this.buffer_manager = buffer_manager;

			// Create the store resource object for this resource name
			this.store_resource = buffer_manager.CreateResource(resource_name);
		}


		// ---------- JournalledFileStore methods ----------

		///<summary>
		/// Deletes this store from the file system.
		///</summary>
		/// <remarks>
		/// This operation should only be used when the store is NOT open.
		/// </remarks>
		///<returns></returns>
		public bool Delete() {
			store_resource.Delete();
			return true;
		}

		/// <summary>
		/// Returns true if this store exists in the file system.
		/// </summary>
		public bool Exists {
			get { return store_resource.Exists; }
		}

		public override void LockForWrite() {
			try {
				buffer_manager.LockForWrite();
			} catch (ThreadInterruptedException e) {
				throw new ApplicationException("Interrupted: " + e.Message);
			}
		}

		public override void UnlockForWrite() {
			buffer_manager.UnlockForWrite();
		}

		public override void CheckPoint() {
		}

		// ---------- Implemented from AbstractStore ----------

		/// <inheritdoc/>
		protected override void InternalOpen(bool read_only) {
			store_resource.Open(read_only);
		}

		/// <inheritdoc/>
		protected override void InternalClose() {
			buffer_manager.Close(store_resource);
		}

		/// <inheritdoc/>
		protected override int ReadByteFrom(long position) {
			return buffer_manager.ReadByteFrom(store_resource, position);
		}

		/// <inheritdoc/>
		protected override int ReadByteArrayFrom(long position, byte[] buf, int off, int len) {
			return buffer_manager.ReadByteArrayFrom(store_resource,
													position, buf, off, len);
		}

		/// <inheritdoc/>
		protected override void WriteByteTo(long position, int b) {
			buffer_manager.WriteByteTo(store_resource, position, b);
		}

		protected override void WriteByteArrayTo(long position,
								 byte[] buf, int off, int len) {
			buffer_manager.WriteByteArrayTo(store_resource,
											position, buf, off, len);
		}

		/// <inheritdoc/>
		protected override long EndOfDataAreaPointer {
			get { return buffer_manager.GetDataAreaSize(store_resource); }
		}

		/// <inheritdoc/>
		protected override void SetDataAreaSize(long new_size) {
			buffer_manager.SetDataAreaSize(store_resource, new_size);
		}

		// For diagnosis
		/// <inheritdoc/>
		public override String ToString() {
			return "[ JournalledFileStore: " + resource_name + " ]";
		}
	}
}