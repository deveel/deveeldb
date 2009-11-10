namespace Deveel.Data {
	partial class Editor {
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
			this.textEditor = new ICSharpCode.TextEditor.TextEditorControl();
			this.panel1 = new System.Windows.Forms.Panel();
			this.lblEditorInfo = new System.Windows.Forms.Label();
			this.contextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
			this.panel1.SuspendLayout();
			this.SuspendLayout();
			// 
			// textEditor
			// 
			this.textEditor.Dock = System.Windows.Forms.DockStyle.Fill;
			this.textEditor.IsReadOnly = false;
			this.textEditor.Location = new System.Drawing.Point(0, 0);
			this.textEditor.Name = "textEditor";
			this.textEditor.Size = new System.Drawing.Size(410, 372);
			this.textEditor.TabIndex = 4;
			this.textEditor.Text = "text";
			// 
			// panel1
			// 
			this.panel1.Controls.Add(this.lblEditorInfo);
			this.panel1.Dock = System.Windows.Forms.DockStyle.Bottom;
			this.panel1.Location = new System.Drawing.Point(0, 351);
			this.panel1.Name = "panel1";
			this.panel1.Size = new System.Drawing.Size(410, 21);
			this.panel1.TabIndex = 1;
			// 
			// lblEditorInfo
			// 
			this.lblEditorInfo.AutoSize = true;
			this.lblEditorInfo.Location = new System.Drawing.Point(3, 4);
			this.lblEditorInfo.Name = "lblEditorInfo";
			this.lblEditorInfo.Size = new System.Drawing.Size(62, 13);
			this.lblEditorInfo.TabIndex = 0;
			this.lblEditorInfo.Text = "lblEditorInfo";
			// 
			// contextMenuStrip
			// 
			this.contextMenuStrip.Name = "contextMenuStrip";
			this.contextMenuStrip.Size = new System.Drawing.Size(61, 4);
			// 
			// Editor
			// 
			this.ClientSize = new System.Drawing.Size(410, 372);
			this.Controls.Add(this.panel1);
			this.Controls.Add(this.textEditor);
			this.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.Name = "Editor";
			this.Load += new System.EventHandler(this.Editor_Load);
			this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.Editor_FormClosing);
			this.panel1.ResumeLayout(false);
			this.panel1.PerformLayout();
			this.ResumeLayout(false);

		}

		#endregion

		private ICSharpCode.TextEditor.TextEditorControl textEditor;
		private System.Windows.Forms.Panel panel1;
		private System.Windows.Forms.Label lblEditorInfo;
		private System.Windows.Forms.ContextMenuStrip contextMenuStrip;
	}
}
