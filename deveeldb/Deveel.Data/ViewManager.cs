// 
//  ViewManager.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
using System.Collections;

using Deveel.Data.Client;
using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// A <see cref="DatabaseConnection">database connection</see> view manager.
	/// </summary>
	/// <remarks>
	/// This controls adding, updating, deleting, and processing views 
	/// inside the system view table.
	/// </remarks>
	public class ViewManager {
		/// <summary>
		/// The DatabaseConnection.
		/// </summary>
		private readonly DatabaseConnection connection;

		/// <summary>
		/// The context.
		/// </summary>
		private readonly DatabaseQueryContext context;

		/// <summary>
		/// Set to true when the connection makes changes to the view table 
		/// through this manager.
		/// </summary>
		private bool view_table_changed;

		/// <summary>
		/// A local cache of ViewDef objects mapped by row id in the system view
		/// table.  This cache is invalidated when changes are committed to the system
		/// view table.
		/// </summary>
		private readonly Hashtable local_cache;

		internal ViewManager(DatabaseConnection connection) {
			this.connection = connection;
			context = new DatabaseQueryContext(connection);
			local_cache = new Hashtable();
			view_table_changed = false;

			// Attach a cache backed on the VIEW table which will invalidate the
			// connection cache whenever the view table is modified.
			connection.AttachTableBackedCache(new TableBackedCacheImpl(this, Database.SysView));

		}

		private class TableBackedCacheImpl : TableBackedCache {
			public TableBackedCacheImpl(ViewManager manager, TableName table)
				: base(table) {
				this.manager = manager;
			}

			private readonly ViewManager manager;

			internal override void PurgeCache(IntegerVector added_rows, IntegerVector removed_rows) {
				// If there were changed then invalidate the cache
				if (manager.view_table_changed) {
					manager.InvalidateViewCache();
					manager.view_table_changed = false;
				}
					// Otherwise, if there were committed added or removed changes also
					// invalidate the cache,
				else if ((added_rows != null && added_rows.Count > 0) ||
						 (removed_rows != null && removed_rows.Count > 0)) {
					manager.InvalidateViewCache();
				}
			}
		}

		/// <summary>
		/// Returns the local cache of ViewDef objects.
		/// </summary>
		/// <remarks>
		/// This cache is mapped from row_id to view object. The cache 
		/// is invalidated when changes are committed to the system 
		/// view table.
		/// </remarks>
		private Hashtable ViewCache {
			get { return local_cache; }
		}

		/// <summary>
		/// Invalidates the view cache.
		/// </summary>
		private void InvalidateViewCache() {
			local_cache.Clear();
		}

		/// <summary>
		/// Gets a view for the given name within the given <see cref="Database.SysView"/> table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="view_name">The name of the view to return.</param>
		/// <returns>
		/// Returns a <see cref="Table"/> containing informations about the 
		/// view.
		/// </returns>
		/// <exception cref="StatementException">
		/// If multiple views were found for the given <paramref name="view_name"/>.
		/// </exception>
		private Table FindViewEntry(Table table, TableName view_name) {
			Operator EQUALS = Operator.Get("=");

			Variable schemav = table.GetResolvedVariable(0);
			Variable namev = table.GetResolvedVariable(1);

			Table t = table.SimpleSelect(context, namev, EQUALS,
							  new Expression(TObject.GetString(view_name.Name)));
			t = t.ExhaustiveSelect(context, Expression.Simple(
						  schemav, EQUALS, TObject.GetString(view_name.Schema)));

			// This should be at most 1 row in size
			if (t.RowCount > 1) {
				throw new Exception(
								"Assert failed: multiple view entries for " + view_name);
			}

			// Return the entries found.
			return t;

		}

		/// <summary>
		/// Checks the existence of a view.
		/// </summary>
		/// <param name="view_name">The name of the view to check.</param>
		/// <returns>
		/// Returns <b>true</b> if a view exists within the underlying session
		/// for the given <paramref name="view_name"/>, otherwise <b>false</b>.
		/// </returns>
		/// <exception cref="StatementException">
		/// If multiple views were found for the given <paramref name="view_name"/>.
		/// </exception>
		public bool ViewExists(TableName view_name) {
			DataTable table = connection.GetTable(Database.SysView);
			return FindViewEntry(table, view_name).RowCount == 1;

		}

		/// <summary>
		/// Defines a new view.
		/// </summary>
		/// <param name="view">The view meta informations for defining the 
		/// new view.</param>
		/// <param name="query">The query forming the view.</param>
		/// <param name="user">The user owning the view.</param>
		/// <remarks>
		/// If a view has previously been defined for the given name,
		/// then it will be overwritten.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If multiple views were found for the given view name.
		/// </exception>
		public void DefineView(ViewDef view, SQLQuery query, User user) {
			DataTableDef data_table_def = view.DataTableDef;
			DataTable view_table = connection.GetTable(Database.SysView);

			TableName view_name = data_table_def.TableName;

			// Create the view record
			RowData rdat = new RowData(view_table);
			rdat.SetColumnDataFromObject(0, data_table_def.Schema);
			rdat.SetColumnDataFromObject(1, data_table_def.Name);
			rdat.SetColumnDataFromObject(2, query.SerializeToBlob());
			rdat.SetColumnDataFromObject(3, view.SerializeToBlob());
			rdat.SetColumnDataFromObject(4, user.UserName);

			// Find the entry from the view that equals this name
			Table t = FindViewEntry(view_table, view_name);

			// Delete the entry if it already exists.
			if (t.RowCount == 1) {
				view_table.Delete(t);
			}

			// Insert the new view entry in the system view table
			view_table.Add(rdat);

			// Notify that this database object has been successfully created.
			connection.DatabaseObjectCreated(view_name);

			// Change to the view table
			view_table_changed = true;

		}

		/// <summary>
		/// Deletes a view within the underlying session.
		/// </summary>
		/// <param name="view_name">The name of the view to delete.</param>
		/// <returns>
		/// Returns <b>true</b> if the view was successfully deleted, or
		/// <b>false</b> if failed or none views for the given <paramref name="view_name"/>
		/// was found.
		/// </returns>
		/// <exception cref="StatementException">
		/// If multiple views were found for the given <paramref name="view_name"/>.
		/// </exception>
		public bool DeleteView(TableName view_name) {
			DataTable table = connection.GetTable(Database.SysView);

			// Find the entry from the view table that equal this name
			Table t = FindViewEntry(table, view_name);

			// No entries so return false
			if (t.RowCount == 0) {
				return false;
			}

			table.Delete(t);

			// Notify that this database object has been successfully dropped.
			connection.DatabaseObjectDropped(view_name);

			// Change to the view table
			view_table_changed = true;

			// Return that 1 or more entries were dropped.
			return true;
		}

		/// <summary>
		/// Creates a view for the given view name in the table.
		/// </summary>
		/// <param name="cache">The view cache</param>
		/// <param name="view_table"></param>
		/// <param name="view_name">The name of the view to create.</param>
		/// <remarks>
		/// The access is cached through the given <paramref name="cache"/>.
		/// <para>
		/// We assume the access to the cache is limited to the current thread
		/// calling this method. We don't lock over the cache at any time.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="ViewDef"/> for the given <paramref name="view_name"/>.
		/// </returns>
		/// <exception cref="StatementException">
		/// If none view was found for the given <paramref name="view_name"/>.
		/// </exception>
		private static ViewDef GetViewDef(IDictionary cache, ITableDataSource view_table, TableName view_name) {
			IRowEnumerator e = view_table.GetRowEnumerator();
			while (e.MoveNext()) {
				int row = e.RowIndex;

				String c_schema =
							  view_table.GetCellContents(0, row).Object.ToString();
				String c_name =
							  view_table.GetCellContents(1, row).Object.ToString();

				if (view_name.Schema.Equals(c_schema) &&
					view_name.Name.Equals(c_name)) {

					Object cache_key = row;
					ViewDef view_def = (ViewDef)cache[cache_key];

					if (view_def == null) {
						// Not in the cache, so deserialize it and WriteByte it in the cache.
						IBlobAccessor blob =
							  (IBlobAccessor)view_table.GetCellContents(3, row).Object;
						// Derserialize the blob
						view_def = ViewDef.DeserializeFromBlob(blob);
						// Put this in the cache....
						cache[cache_key] = view_def;

					}
					return view_def;
				}

			}

			throw new StatementException("View '" + view_name + "' not found.");
		}

		/// <summary>
		/// Creates a <see cref="ViewDef"/> object for the given index 
		/// value in the table.
		/// </summary>
		/// <param name="cache"></param>
		/// <param name="view_table"></param>
		/// <param name="index"></param>
		/// <remarks>
		/// The access is cached through the given <see cref="IDictionary"/> object.
		/// <para>
		/// We assume the access to the cache is limited to the current thread
		/// calling this method.  We don't synchronize over the cache at any time.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		private static ViewDef GetViewDef(IDictionary cache, ITableDataSource view_table, int index) {
			IRowEnumerator e = view_table.GetRowEnumerator();
			int i = 0;
			while (e.MoveNext()) {
				int row = e.RowIndex;

				if (i == index) {
					Object cache_key = row;
					ViewDef view_def = (ViewDef)cache[cache_key];

					if (view_def == null) {
						// Not in the cache, so deserialize it and WriteByte it in the cache.
						IBlobAccessor blob =
							  (IBlobAccessor)view_table.GetCellContents(3, row).Object;
						// Derserialize the blob
						view_def = ViewDef.DeserializeFromBlob(blob);
						// Put this in the cache....
						cache[cache_key] = view_def;

					}
					return view_def;
				}

				++i;
			}
			throw new ApplicationException("Index out of range.");
		}

		///<summary>
		/// Returns a freshly deserialized <see cref="IQueryPlanNode"/> 
		/// object for the given view object.
		///</summary>
		///<param name="view_name"></param>
		///<returns></returns>
		public IQueryPlanNode CreateViewQueryPlanNode(TableName view_name) {
			DataTable table = connection.GetTable(Database.SysView);
			return GetViewDef(local_cache, table, view_name).QueryPlanNode;
		}

		/// <summary>
		/// Gets the internal table for a given view manager.
		/// </summary>
		/// <param name="manager"></param>
		/// <param name="transaction"></param>
		/// <remarks>
		/// This is used to model all views as regular tables accessible 
		/// within a transaction.
		/// <para>
		/// Note that <paramref name="manager"/> can be <b>null</b> if there is no backing
		/// view manager. The view manager is intended as a cache to improve the
		/// access speed of the manager.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an <see cref="IInternalTableInfo"/> used to model the list 
		/// of views that are accessible within the underlying transaction.
		/// </returns>
		internal static IInternalTableInfo CreateInternalTableInfo(ViewManager manager, Transaction transaction) {
			return new ViewInternalTableInfo(manager, transaction);
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// An object that models the list of views as table objects 
		/// in a transaction.
		/// </summary>
		private sealed class ViewInternalTableInfo : InternalTableInfo2 {
			private readonly ViewManager view_manager;
			private readonly Hashtable view_cache;

			internal ViewInternalTableInfo(ViewManager manager, Transaction transaction)
				: base(transaction, Database.SysView) {
				view_manager = manager;
				view_cache = view_manager == null ? new Hashtable() : view_manager.ViewCache;
			}

			public override String GetTableType(int i) {
				return "VIEW";
			}

			public override DataTableDef GetDataTableDef(int i) {
				return GetViewDef(view_cache,
				                  transaction.GetTable(Database.SysView), i).DataTableDef;
			}

			public override IMutableTableDataSource CreateInternalTable(int i) {
				throw new Exception("Not supported for views.");
			}

		}

	}
}