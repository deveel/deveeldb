// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Store;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// A store that manages the current state of all tables in a 
	/// <see cref="TableDataConglomerate"/>.
	/// </summary>
	/// <remarks>
	/// It persistantly manages three pieces of information about a conglomerate
	/// <list type="number">
	/// <item>the tables that are visible</item>
	/// <item>the tables that are deleted</item>
	/// <item>a table identification number value assigned to new tables that 
	/// are created</item>
	/// </list>
	/// </remarks>
	internal class StateStore {
		private bool delListChange;

		/// <summary>
		/// Pointer to the delete table area in the store.
		/// </summary>
		private long delAreaPointer;

		/// <summary>
		/// The list of deleted state resources.
		/// </summary>
		private List<StateResource> deleteList;

		/// <summary>
		/// The header area of the state store. The format of the header area is:
		///   MAGIC(4)
		///   RESERVED(4)
		///   TABLE_ID(8)
		///   VISIBLE_TABLES_POINTER(8)
		///   DELETED_TABLES_POINTER(8)
		/// </summary>
		private IMutableArea headerArea;

		/// <summary>
		/// The MAGIC value used for state header areas.
		/// </summary>
		private const int MAGIC = 0x0BAC8001;

		/// <summary>
		/// The Store object this state store wraps around.
		/// </summary>
		private readonly IStore store;

		/// <summary>
		/// The current table identifier.
		/// </summary>
		private int currentTableId;
		/// <summary>
		/// Set to true if the visible list was changed.
		/// </summary>
		private bool visListChange;

		/// <summary>
		/// Pointer to the visible table area in the store.
		/// </summary>
		private long visAreaPointer;

		/// <summary>
		/// The list of visible state resources.
		/// </summary>
		private List<StateResource> visibleList;


		public StateStore(IStore store) {
			this.store = store;
			visListChange = false;
			delListChange = false;
		}


		/// <summary>
		/// Removes the given resource from the list.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="name"></param>
		private static void RemoveResource(IList<StateResource> list, String name) {
			int sz = list.Count;
			for (int i = 0; i < sz; ++i) {
				StateResource resource = list[i];
				if (name.Equals(resource.Name)) {
					list.RemoveAt(i);
					return;
				}
			}
			throw new Exception("Couldn't find resource '" + name + "' in list.");
		}

		/// <summary>
		/// Reads the state resource list from the given area in the store.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="pointer"></param>
		private void ReadStateResourceList(IList<StateResource> list, long pointer) {
			BinaryReader reader = new BinaryReader(store.GetAreaInputStream(pointer), Encoding.Unicode);
			reader.ReadInt32(); // version
			int count = (int) reader.ReadInt64();
			for (int i = 0; i < count; ++i) {
				long tableId = reader.ReadInt64();
				string name = reader.ReadString();
				list.Add(new StateResource(tableId, name));
			}
			reader.Close();
		}

		/// <summary>
		/// Writes the state resource list to the given area in the store.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="writer"></param>
		private static void WriteStateResourceList(IList<StateResource> list, BinaryWriter writer) {
			writer.Write(1);	// version
			int sz = list.Count;
			writer.Write((long) sz);
			for (int i = 0; i < sz; ++i) {
				StateResource resource = list[i];
				writer.Write(resource.TableId);
				writer.Write(resource.Name);
			}
		}

		/// <summary>
		/// Writes the given list to the store and returns a pointer to the 
		/// area once the write has finished.
		/// </summary>
		/// <param name="list"></param>
		/// <returns></returns>
		private long WriteListToStore(List<StateResource> list) {
			MemoryStream stream = new MemoryStream();
			BinaryWriter writer = new BinaryWriter(stream, Encoding.Unicode);
			WriteStateResourceList(list, writer);
			writer.Flush();
			writer.Close();

			byte[] buf = stream.ToArray();

			IAreaWriter a = store.CreateArea(buf.Length);
			long listP = a.Id;
			a.Write(buf);
			a.Finish();

			return listP;
		}

		/// <summary>
		/// Creates the state store in the store and returns a pointer to the 
		/// header used later for initializing the state.
		/// </summary>
		/// <returns></returns>
		public long Create() {
			lock (this) {
				// Allocate empty visible and deleted tables area
				IAreaWriter visTablesArea = store.CreateArea(12);
				IAreaWriter delTablesArea = store.CreateArea(12);
				visAreaPointer = visTablesArea.Id;
				delAreaPointer = delTablesArea.Id;

				// Write empty entries for both of these
				visTablesArea.WriteInt4(1);
				visTablesArea.WriteInt8(0);
				visTablesArea.Finish();
				delTablesArea.WriteInt4(1);
				delTablesArea.WriteInt8(0);
				delTablesArea.Finish();

				// Now allocate an empty state header
				IAreaWriter headerWriter = store.CreateArea(32);
				long headerP = headerWriter.Id;
				headerWriter.WriteInt4(MAGIC);
				headerWriter.WriteInt4(0);
				headerWriter.WriteInt8(0);
				headerWriter.WriteInt8(visAreaPointer);
				headerWriter.WriteInt8(delAreaPointer);
				headerWriter.Finish();

				headerArea = store.GetMutableArea(headerP);

				// Reset currentTableId
				currentTableId = 0;

				visibleList = new List<StateResource>();
				deleteList = new List<StateResource>();

				// Return pointer to the header area
				return headerP;
			}
		}

		/// <summary>
		/// Initializes the state store given a pointer to the header area in 
		/// the store.
		/// </summary>
		/// <param name="headerPointer"></param>
		public void Init(long headerPointer) {
			lock (this) {
				headerArea = store.GetMutableArea(headerPointer);
				int mag_value = headerArea.ReadInt4();
				if (mag_value != MAGIC)
					throw new IOException("Magic value for state header area is incorrect.");

				if (headerArea.ReadInt4() != 0)
					throw new IOException("Unknown version for state header area.");

				currentTableId = (int) headerArea.ReadInt8();
				visAreaPointer = headerArea.ReadInt8();
				delAreaPointer = headerArea.ReadInt8();

				// Setup the visible and delete list
				visibleList = new List<StateResource>();
				deleteList = new List<StateResource>();

				// Read the resource list for the visible and delete list.
				ReadStateResourceList(visibleList, visAreaPointer);
				ReadStateResourceList(deleteList, delAreaPointer);
			}
		}

		/// <summary>
		/// Returns the next table id and increments the currentTableId counter.
		/// </summary>
		/// <returns></returns>
		public int NextTableId() {
			lock (this) {
				int curCounter = currentTableId;
				++currentTableId;

				try {
					store.LockForWrite();

					// Update the state in the file
					headerArea.Position = 8;
					headerArea.WriteInt8(currentTableId);
					// Check out the change
					headerArea.CheckOut();
				} finally {
					store.UnlockForWrite();
				}

				return curCounter;
			}
		}

		/// <summary>
		/// Returns a list of all table resources that are currently in the 
		/// visible list.
		/// </summary>
		/// <returns></returns>
		public IEnumerable<StateResource> GetVisibleList() {
			lock (this) {
				return visibleList.AsReadOnly();
			}
		}

		/// <summary>
		/// Returns a list of all table resources that are currently in the 
		/// delete list.
		/// </summary>
		/// <returns></returns>
		public StateResource[] GetDeleteList() {
			lock (this) {
				return deleteList.ToArray();
			}
		}

		/// <summary>
		/// Returns true if the visible list contains a state resource with the 
		/// given table id value.
		/// </summary>
		/// <param name="tableId"></param>
		/// <returns></returns>
		public bool ContainsVisibleResource(int tableId) {
			lock (this) {
				foreach (StateResource resource in visibleList) {
					if (resource.TableId == tableId)
						return true;
				}
				return false;
			}
		}

		/// <summary>
		/// Adds the given StateResource to the visible table list.
		/// </summary>
		/// <param name="resource"></param>
		/// <remarks>
		/// This does not persist the state. To persist this change a call to 
		/// <see cref="Commit"/> must be called.
		/// </remarks>
		public void AddVisibleResource(StateResource resource) {
			lock (this) {
				visibleList.Add(resource);
				visListChange = true;
			}
		}

		/// <summary>
		/// Adds the given StateResource to the delete table list.
		/// </summary>
		/// <param name="resource"></param>
		/// <remarks>
		/// This does not persist the state. To persist this change a call to 
		/// <see cref="Commit"/> must be called.
		/// </remarks>
		public void AddDeleteResource(StateResource resource) {
			lock (this) {
				deleteList.Add(resource);
				delListChange = true;
			}
		}

		/// <summary>
		/// Removes the resource with the given name from the visible table list.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// This does not persist the state. To persist this change a call to 
		/// <see cref="Commit"/> must be called.
		/// </remarks>
		public void RemoveVisibleResource(string name) {
			lock (this) {
				RemoveResource(visibleList, name);
				visListChange = true;
			}
		}

		/// <summary>
		/// Removes the resource with the given name from the delete table list.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// This does not persist the state. To persist this change a call to 
		/// <see cref="Commit"/> must be called.
		/// </remarks>
		public void RemoveDeleteResource(string name) {
			lock (this) {
				RemoveResource(deleteList, name);
				delListChange = true;
			}
		}

		/// <summary>
		/// Commits the current state to disk so that it makes a persistent 
		/// change to the state.
		/// </summary>
		/// <remarks>
		/// A further call to <i>Synch</i> will synchronize the file. This will 
		/// only commit changes if there were modifications to the state.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if this commit caused any changes to the 
		/// persistant state, otherwise <b>false</b>.
		/// </returns>
		public bool Commit() {
			lock (this) {
				bool changes = false;
				long newVisP = visAreaPointer;
				long newDelP = delAreaPointer;

				try {
					store.LockForWrite();

					// If the lists changed, then Write new state areas to the store.
					if (visListChange) {
						newVisP = WriteListToStore(visibleList);
						visListChange = false;
						changes = true;
					}
					if (delListChange) {
						newDelP = WriteListToStore(deleteList);
						delListChange = false;
						changes = true;
					}
					// Commit the changes,
					if (changes) {
						headerArea.Position = 16;
						headerArea.WriteInt8(newVisP);
						headerArea.WriteInt8(newDelP);
						// Check out the change.
						headerArea.CheckOut();
						if (visAreaPointer != newVisP) {
							store.DeleteArea(visAreaPointer);
							visAreaPointer = newVisP;
						}
						if (delAreaPointer != newDelP) {
							store.DeleteArea(delAreaPointer);
							delAreaPointer = newDelP;
						}
					}
				} finally {
					store.UnlockForWrite();
				}

				return changes;
			}
		}

		// ---------- Inner classes ----------

		#region StateResource

		/// <summary>
		/// Represents a single StateResource in either a visible or delete list in
		/// this state file.
		/// </summary>
		internal sealed class StateResource {
			/// <summary>
			/// The unique name given to the resource to distinguish it 
			/// from all other resources.
			/// </summary>
			private readonly string name;

			/// <summary>
			/// The unique identifier for the resource.
			/// </summary>
			private readonly long tableId;

			public StateResource(long tableId, string name) {
				this.tableId = tableId;
				this.name = name;
			}

			/// <summary>
			/// The unique name given to the resource to distinguish it 
			/// from all other resources.
			/// </summary>
			public string Name {
				get { return name; }
			}

			/// <summary>
			/// The unique identifier for the resource.
			/// </summary>
			public long TableId {
				get { return tableId; }
			}
		}

		#endregion
	}
}