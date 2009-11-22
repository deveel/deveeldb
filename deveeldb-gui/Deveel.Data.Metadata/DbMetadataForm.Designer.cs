namespace Deveel.Data.Metadata {
	partial class DbMetadataForm {
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
			System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(DbMetadataForm));
			this.metaTreeView = new System.Windows.Forms.TreeView();
			this.SuspendLayout();
			// 
			// metaTreeView
			// 
			this.metaTreeView.Dock = System.Windows.Forms.DockStyle.Fill;
			this.metaTreeView.Location = new System.Drawing.Point(0, 0);
			this.metaTreeView.Name = "metaTreeView";
			this.metaTreeView.Size = new System.Drawing.Size(371, 418);
			this.metaTreeView.TabIndex = 0;
			// 
			// DbMetadataForm
			// 
			this.ClientSize = new System.Drawing.Size(371, 418);
			this.Controls.Add(this.metaTreeView);
			this.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
			this.Name = "DbMetadataForm";
			this.TabText = "DB Metadata";
			this.Text = "Database Metadata";
			this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.TreeView metaTreeView;
	}
}