using System;
using System.Windows.Forms;

using Deveel.Data.DbModel;

using WeifenLuo.WinFormsUI.Docking;

namespace Deveel.Data.Metadata {
	public partial class DbMetadataForm : DockContent, IDbMetadataProvider {
		public DbMetadataForm() {
			InitializeComponent();
		}

		private readonly static object rootTag = new object();
		private static readonly object tablesTag = new object();
		private static readonly object viewsTag = new object();
		internal IDbMetadataService metaDataService;
		private DbDatabase model;
		private bool populated;
		private TreeNode rightClickedNode;
		private ISqlStatementFormatter sqlFormatter;
		private TreeNode tablesNode;
		private TreeNode viewsNode;


		#region Implementation of IDbMetadataProvider

		public string SelectedTable {
			get { throw new NotImplementedException(); }
		}

		public DbSchema Schema {
			get { throw new NotImplementedException(); }
		}

		void IDbMetadataProvider.Load() {
			throw new NotImplementedException();
		}

		void IDbMetadataProvider.Close() {
			
		}

		#endregion
	}
}