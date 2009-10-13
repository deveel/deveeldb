//  
//  StateStore.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.IO;
using System.Text;

using Deveel.Data.Store;

namespace Deveel.Data {
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
		private bool del_list_change;
		/// <summary>
		/// Pointer to the delete table area in the store.
		/// </summary>
		private long del_p;
		/// <summary>
		/// The list of deleted state resources.
		/// </summary>
		private ArrayList delete_list;

		/// <summary>
		/// The header area of the state store. The format of the header area is:
		///   MAGIC(4)
		///   RESERVED(4)
		///   TABLE_ID(8)
		///   VISIBLE_TABLES_POINTER(8)
		///   DELETED_TABLES_POINTER(8)
		/// </summary>
		private IMutableArea header_area;
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
		private int table_id;
		/// <summary>
		/// Set to true if the visible list was changed.
		/// </summary>
		private bool vis_list_change;

		/// <summary>
		/// Pointer to the visible table area in the store.
		/// </summary>
		private long vis_p;

		/// <summary>
		/// The list of visible state resources.
		/// </summary>
		private ArrayList visible_list;


		public StateStore(IStore store) {
			this.store = store;
			vis_list_change = false;
			del_list_change = false;
		}


		/// <summary>
		/// Removes the given resource from the list.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="name"></param>
		private static void RemoveResource(ArrayList list, String name) {
			int sz = list.Count;
			for (int i = 0; i < sz; ++i) {
				StateResource resource = (StateResource)list[i];
				if (name.Equals(resource.name)) {
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
		private void ReadStateResourceList(IList list, long pointer) {
			BinaryReader d_in = new BinaryReader(store.GetAreaInputStream(pointer), Encoding.Unicode);
			int version = d_in.ReadInt32(); // version
			int count = (int) d_in.ReadInt64();
			for (int i = 0; i < count; ++i) {
				long table_id = d_in.ReadInt64();
				String name = d_in.ReadString();
				StateResource resource = new StateResource(table_id, name);
				list.Add(resource);
			}
			d_in.Close();
		}

		/// <summary>
		/// Writes the state resource list to the given area in the store.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="d_out"></param>
		private static void WriteStateResourceList(IList list, BinaryWriter d_out) {
			d_out.Write(1);
			int sz = list.Count;
			d_out.Write((long) sz);
			for (int i = 0; i < sz; ++i) {
				StateResource resource = (StateResource)list[i];
				d_out.Write(resource.table_id);
				d_out.Write(resource.name);
			}
		}

		/// <summary>
		/// Writes the given list to the store and returns a pointer to the 
		/// area once the write has finished.
		/// </summary>
		/// <param name="list"></param>
		/// <returns></returns>
		private long WriteListToStore(ArrayList list) {
			MemoryStream bout = new MemoryStream();
			BinaryWriter d_out = new BinaryWriter(bout, Encoding.Unicode);
			WriteStateResourceList(list, d_out);
			d_out.Flush();
			d_out.Close();

			byte[] buf = bout.ToArray();

			IAreaWriter a = store.CreateArea(buf.Length);
			long list_p = a.Id;
			a.Write(buf);
			a.Finish();

			return list_p;
		}

		/// <summary>
		/// Creates the state store in the store and returns a pointer to the 
		/// header used later for initializing the state.
		/// </summary>
		/// <returns></returns>
		public long Create() {
			lock (this) {
				// Allocate empty visible and deleted tables area
				IAreaWriter vis_tables_area = store.CreateArea(12);
				IAreaWriter del_tables_area = store.CreateArea(12);
				vis_p = vis_tables_area.Id;
				del_p = del_tables_area.Id;

				// Write empty entries for both of these
				vis_tables_area.WriteInt4(1);
				vis_tables_area.WriteInt8(0);
				vis_tables_area.Finish();
				del_tables_area.WriteInt4(1);
				del_tables_area.WriteInt8(0);
				del_tables_area.Finish();

				// Now allocate an empty state header
				IAreaWriter header_writer = store.CreateArea(32);
				long header_p = header_writer.Id;
				header_writer.WriteInt4(MAGIC);
				header_writer.WriteInt4(0);
				header_writer.WriteInt8(0);
				header_writer.WriteInt8(vis_p);
				header_writer.WriteInt8(del_p);
				header_writer.Finish();

				header_area = store.GetMutableArea(header_p);

				// Reset table_id
				table_id = 0;

				visible_list = new ArrayList();
				delete_list = new ArrayList();

				// Return pointer to the header area
				return header_p;
			}
		}

		/// <summary>
		/// Initializes the state store given a pointer to the header area in 
		/// the store.
		/// </summary>
		/// <param name="header_p"></param>
		public void init(long header_p) {
			lock (this) {
				header_area = store.GetMutableArea(header_p);
				int mag_value = header_area.ReadInt4();
				if (mag_value != MAGIC) {
					throw new IOException("Magic value for state header area is incorrect.");
				}
				if (header_area.ReadInt4() != 0) {
					throw new IOException("Unknown version for state header area.");
				}
				table_id = (int) header_area.ReadInt8();
				vis_p = header_area.ReadInt8();
				del_p = header_area.ReadInt8();

				// Setup the visible and delete list
				visible_list = new ArrayList();
				delete_list = new ArrayList();

				// Read the resource list for the visible and delete list.
				ReadStateResourceList(visible_list, vis_p);
				ReadStateResourceList(delete_list, del_p);
			}
		}

		/// <summary>
		/// Returns the next table id and increments the table_id counter.
		/// </summary>
		/// <returns></returns>
		public int NextTableId() {
			lock (this) {
				int cur_counter = table_id;
				++table_id;

				try {
					store.LockForWrite();

					// Update the state in the file
					header_area.Position = 8;
					header_area.WriteInt8(table_id);
					// Check out the change
					header_area.CheckOut();
				} finally {
					store.UnlockForWrite();
				}

				return cur_counter;
			}
		}

		/// <summary>
		/// Returns a list of all table resources that are currently in the 
		/// visible list.
		/// </summary>
		/// <returns></returns>
		internal StateResource[] GetVisibleList() {
			lock (this) {
				return (StateResource[]) visible_list.ToArray(typeof (StateResource));
			}
		}

		/// <summary>
		/// Returns a list of all table resources that are currently in the 
		/// delete list.
		/// </summary>
		/// <returns></returns>
		internal StateResource[] GetDeleteList() {
			lock (this) {
				return (StateResource[]) delete_list.ToArray(typeof (StateResource));
			}
		}

		/// <summary>
		/// Returns true if the visible list contains a state resource with the 
		/// given table id value.
		/// </summary>
		/// <param name="table_id"></param>
		/// <returns></returns>
		public bool ContainsVisibleResource(int table_id) {
			lock (this) {
				int sz = visible_list.Count;
				for (int i = 0; i < sz; ++i) {
					if (((StateResource) visible_list[i]).table_id == table_id) {
						return true;
					}
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
				visible_list.Add(resource);
				vis_list_change = true;
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
				delete_list.Add(resource);
				del_list_change = true;
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
		public void RemoveVisibleResource(String name) {
			lock (this) {
				RemoveResource(visible_list, name);
				vis_list_change = true;
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
		public void RemoveDeleteResource(String name) {
			lock (this) {
				RemoveResource(delete_list, name);
				del_list_change = true;
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
				long new_vis_p = vis_p;
				long new_del_p = del_p;

				try {
					store.LockForWrite();

					// If the lists changed, then Write new state areas to the store.
					if (vis_list_change) {
						new_vis_p = WriteListToStore(visible_list);
						vis_list_change = false;
						changes = true;
					}
					if (del_list_change) {
						new_del_p = WriteListToStore(delete_list);
						del_list_change = false;
						changes = true;
					}
					// Commit the changes,
					if (changes) {
						header_area.Position = 16;
						header_area.WriteInt8(new_vis_p);
						header_area.WriteInt8(new_del_p);
						// Check out the change.
						header_area.CheckOut();
						if (vis_p != new_vis_p) {
							store.DeleteArea(vis_p);
							vis_p = new_vis_p;
						}
						if (del_p != new_del_p) {
							store.DeleteArea(del_p);
							del_p = new_del_p;
						}
					}
				} finally {
					store.UnlockForWrite();
				}

				return changes;
			}
		}

		// ---------- Inner classes ----------

		#region Nested type: StateResource

		/// <summary>
		/// Represents a single StateResource in either a visible or delete list in
		/// this state file.
		/// </summary>
		internal sealed class StateResource {
			/// <summary>
			/// The unique name given to the resource to distinguish it 
			/// from all other resources.
			/// </summary>
			public String name;

			/// <summary>
			/// The unique identifier for the resource.
			/// </summary>
			public long table_id;

			public StateResource(long table_id, String name) {
				this.table_id = table_id;
				this.name = name;
			}
		}

		#endregion
	}
}