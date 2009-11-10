namespace Deveel.Data {
	partial class QueryEditor {
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.IContainer components = null;

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		/// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
		protected override void Dispose(bool disposing) {
			if (disposing && (components != null)) {
				components.Dispose();
			}
			base.Dispose(disposing);
		}

		#region Windows Form Designer generated code

		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent() {
			this.components = new System.ComponentModel.Container();
			System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(QueryEditor));
			this.splitContainer = new System.Windows.Forms.SplitContainer();
			this.txtQuery = new ICSharpCode.TextEditor.TextEditorControl();
			this.resultTabControl = new System.Windows.Forms.TabControl();
			this.tabResult = new System.Windows.Forms.TabPage();
			this.grdResult = new System.Windows.Forms.DataGridView();
			this.contextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
			this.editorContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
			this.queryBackgroundWorker = new System.ComponentModel.BackgroundWorker();
			this.resultContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
			this.selectAllToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.toolStripMenuItem2 = new System.Windows.Forms.ToolStripSeparator();
			this.copySelectedToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.splitContainer.Panel1.SuspendLayout();
			this.splitContainer.Panel2.SuspendLayout();
			this.splitContainer.SuspendLayout();
			this.resultTabControl.SuspendLayout();
			this.tabResult.SuspendLayout();
			((System.ComponentModel.ISupportInitialize)(this.grdResult)).BeginInit();
			this.resultContextMenuStrip.SuspendLayout();
			this.SuspendLayout();
			// 
			// splitContainer
			// 
			this.splitContainer.Dock = System.Windows.Forms.DockStyle.Fill;
			this.splitContainer.Location = new System.Drawing.Point(0, 0);
			this.splitContainer.Name = "splitContainer";
			this.splitContainer.Orientation = System.Windows.Forms.Orientation.Horizontal;
			// 
			// splitContainer.Panel1
			// 
			this.splitContainer.Panel1.Controls.Add(this.txtQuery);
			// 
			// splitContainer.Panel2
			// 
			this.splitContainer.Panel2.Controls.Add(this.resultTabControl);
			this.splitContainer.Size = new System.Drawing.Size(1042, 476);
			this.splitContainer.SplitterDistance = 231;
			this.splitContainer.TabIndex = 0;
			// 
			// txtQuery
			// 
			this.txtQuery.ContextMenuStrip = this.contextMenuStrip;
			this.txtQuery.Dock = System.Windows.Forms.DockStyle.Fill;
			this.txtQuery.EnableFolding = false;
			this.txtQuery.IsReadOnly = false;
			this.txtQuery.Location = new System.Drawing.Point(0, 0);
			this.txtQuery.Name = "txtQuery";
			this.txtQuery.ShowEOLMarkers = true;
			this.txtQuery.ShowSpaces = true;
			this.txtQuery.ShowTabs = true;
			this.txtQuery.ShowVRuler = false;
			this.txtQuery.Size = new System.Drawing.Size(1042, 231);
			this.txtQuery.TabIndex = 0;
			// 
			// resultTabControl
			// 
			this.resultTabControl.ContextMenuStrip = this.resultContextMenuStrip;
			this.resultTabControl.Controls.Add(this.tabResult);
			this.resultTabControl.Dock = System.Windows.Forms.DockStyle.Fill;
			this.resultTabControl.Location = new System.Drawing.Point(0, 0);
			this.resultTabControl.Name = "resultTabControl";
			this.resultTabControl.SelectedIndex = 0;
			this.resultTabControl.Size = new System.Drawing.Size(1042, 241);
			this.resultTabControl.TabIndex = 0;
			// 
			// tabResult
			// 
			this.tabResult.Controls.Add(this.grdResult);
			this.tabResult.Location = new System.Drawing.Point(4, 22);
			this.tabResult.Name = "tabResult";
			this.tabResult.Padding = new System.Windows.Forms.Padding(3);
			this.tabResult.Size = new System.Drawing.Size(1034, 215);
			this.tabResult.TabIndex = 0;
			this.tabResult.Text = "Result";
			this.tabResult.UseVisualStyleBackColor = true;
			// 
			// grdResult
			// 
			this.grdResult.AllowUserToAddRows = false;
			this.grdResult.AllowUserToDeleteRows = false;
			this.grdResult.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.ColumnHeader;
			this.grdResult.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.grdResult.Dock = System.Windows.Forms.DockStyle.Fill;
			this.grdResult.Location = new System.Drawing.Point(3, 3);
			this.grdResult.Name = "grdResult";
			this.grdResult.Size = new System.Drawing.Size(1028, 209);
			this.grdResult.TabIndex = 0;
			// 
			// contextMenuStrip
			// 
			this.contextMenuStrip.Name = "contextMenuStrip";
			this.contextMenuStrip.Size = new System.Drawing.Size(61, 4);
			// 
			// editorContextMenuStrip
			// 
			this.editorContextMenuStrip.Name = "editorContextMenuStrip";
			this.editorContextMenuStrip.Size = new System.Drawing.Size(61, 4);
			// 
			// queryBackgroundWorker
			// 
			this.queryBackgroundWorker.DoWork += new System.ComponentModel.DoWorkEventHandler(this.queryBackgroundWorker_DoWork);
			this.queryBackgroundWorker.RunWorkerCompleted += new System.ComponentModel.RunWorkerCompletedEventHandler(this.queryBackgroundWorker_RunWorkerCompleted);
			this.queryBackgroundWorker.ProgressChanged += new System.ComponentModel.ProgressChangedEventHandler(this.queryBackgroundWorker_ProgressChanged);
			// 
			// resultContextMenuStrip
			// 
			this.resultContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.selectAllToolStripMenuItem,
            this.toolStripMenuItem2,
            this.copySelectedToolStripMenuItem});
			this.resultContextMenuStrip.Name = "resultContextMenuStrip";
			this.resultContextMenuStrip.Size = new System.Drawing.Size(153, 76);
			// 
			// selectAllToolStripMenuItem
			// 
			this.selectAllToolStripMenuItem.Name = "selectAllToolStripMenuItem";
			this.selectAllToolStripMenuItem.Size = new System.Drawing.Size(152, 22);
			this.selectAllToolStripMenuItem.Text = "Select All";
			this.selectAllToolStripMenuItem.Click += new System.EventHandler(this.selectAllToolStripMenuItem_Click);
			// 
			// toolStripMenuItem2
			// 
			this.toolStripMenuItem2.Name = "toolStripMenuItem2";
			this.toolStripMenuItem2.Size = new System.Drawing.Size(149, 6);
			// 
			// copySelectedToolStripMenuItem
			// 
			this.copySelectedToolStripMenuItem.Name = "copySelectedToolStripMenuItem";
			this.copySelectedToolStripMenuItem.Size = new System.Drawing.Size(152, 22);
			this.copySelectedToolStripMenuItem.Text = "Copy Selected";
			this.copySelectedToolStripMenuItem.Click += new System.EventHandler(this.copySelectedToolStripMenuItem_Click);
			// 
			// QueryEditor
			// 
			this.ClientSize = new System.Drawing.Size(1042, 476);
			this.ContextMenuStrip = this.editorContextMenuStrip;
			this.Controls.Add(this.splitContainer);
			this.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
			this.Name = "QueryEditor";
			this.Text = "Query";
			this.Deactivate += new System.EventHandler(this.QueryEditor_Deactivate);
			this.Load += new System.EventHandler(this.QueryEditor_Load);
			this.Activated += new System.EventHandler(this.QueryEditor_Activated);
			this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.QueryEditor_FormClosing);
			this.splitContainer.Panel1.ResumeLayout(false);
			this.splitContainer.Panel2.ResumeLayout(false);
			this.splitContainer.ResumeLayout(false);
			this.resultTabControl.ResumeLayout(false);
			this.tabResult.ResumeLayout(false);
			((System.ComponentModel.ISupportInitialize)(this.grdResult)).EndInit();
			this.resultContextMenuStrip.ResumeLayout(false);
			this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.SplitContainer splitContainer;
		private ICSharpCode.TextEditor.TextEditorControl txtQuery;
		private System.Windows.Forms.TabControl resultTabControl;
		private System.Windows.Forms.TabPage tabResult;
		private System.Windows.Forms.DataGridView grdResult;
		private System.Windows.Forms.ContextMenuStrip contextMenuStrip;
		private System.Windows.Forms.ContextMenuStrip editorContextMenuStrip;
		private System.ComponentModel.BackgroundWorker queryBackgroundWorker;
		private System.Windows.Forms.ContextMenuStrip resultContextMenuStrip;
		private System.Windows.Forms.ToolStripMenuItem selectAllToolStripMenuItem;
		private System.Windows.Forms.ToolStripSeparator toolStripMenuItem2;
		private System.Windows.Forms.ToolStripMenuItem copySelectedToolStripMenuItem;

	}
}
