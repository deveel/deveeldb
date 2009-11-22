namespace Deveel.Data.Search {
	partial class FindReplaceForm {
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
			this.btnCancel = new System.Windows.Forms.Button();
			this.btnReplace = new System.Windows.Forms.Button();
			this.btnFind = new System.Windows.Forms.Button();
			this.txtFind = new System.Windows.Forms.TextBox();
			this.lblFind = new System.Windows.Forms.Label();
			this.lblReplace = new System.Windows.Forms.Label();
			this.txtReplace = new System.Windows.Forms.TextBox();
			this.SuspendLayout();
			// 
			// btnCancel
			// 
			this.btnCancel.Location = new System.Drawing.Point(267, 93);
			this.btnCancel.Name = "btnCancel";
			this.btnCancel.Size = new System.Drawing.Size(75, 23);
			this.btnCancel.TabIndex = 0;
			this.btnCancel.Text = "&Cancel";
			this.btnCancel.UseVisualStyleBackColor = true;
			// 
			// btnReplace
			// 
			this.btnReplace.Location = new System.Drawing.Point(267, 64);
			this.btnReplace.Name = "btnReplace";
			this.btnReplace.Size = new System.Drawing.Size(75, 23);
			this.btnReplace.TabIndex = 1;
			this.btnReplace.Text = "&Replace";
			this.btnReplace.UseVisualStyleBackColor = true;
			this.btnReplace.Click += new System.EventHandler(this.btnReplace_Click);
			// 
			// btnFind
			// 
			this.btnFind.Location = new System.Drawing.Point(186, 64);
			this.btnFind.Name = "btnFind";
			this.btnFind.Size = new System.Drawing.Size(75, 23);
			this.btnFind.TabIndex = 2;
			this.btnFind.Text = "&Find Next";
			this.btnFind.UseVisualStyleBackColor = true;
			this.btnFind.Click += new System.EventHandler(this.btnFind_Click);
			// 
			// txtFind
			// 
			this.txtFind.Location = new System.Drawing.Point(90, 12);
			this.txtFind.Name = "txtFind";
			this.txtFind.Size = new System.Drawing.Size(252, 20);
			this.txtFind.TabIndex = 3;
			this.txtFind.Leave += new System.EventHandler(this.txtFind_Leave);
			this.txtFind.Enter += new System.EventHandler(this.txtFind_Enter);
			// 
			// lblFind
			// 
			this.lblFind.AutoSize = true;
			this.lblFind.Location = new System.Drawing.Point(25, 15);
			this.lblFind.Name = "lblFind";
			this.lblFind.Size = new System.Drawing.Size(59, 13);
			this.lblFind.TabIndex = 4;
			this.lblFind.Text = "Search for:";
			// 
			// lblReplace
			// 
			this.lblReplace.AutoSize = true;
			this.lblReplace.Location = new System.Drawing.Point(12, 41);
			this.lblReplace.Name = "lblReplace";
			this.lblReplace.Size = new System.Drawing.Size(72, 13);
			this.lblReplace.TabIndex = 5;
			this.lblReplace.Text = "Replace with:";
			// 
			// txtReplace
			// 
			this.txtReplace.Location = new System.Drawing.Point(90, 38);
			this.txtReplace.Name = "txtReplace";
			this.txtReplace.Size = new System.Drawing.Size(252, 20);
			this.txtReplace.TabIndex = 6;
			// 
			// FindReplaceForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(353, 132);
			this.Controls.Add(this.txtReplace);
			this.Controls.Add(this.lblReplace);
			this.Controls.Add(this.lblFind);
			this.Controls.Add(this.txtFind);
			this.Controls.Add(this.btnFind);
			this.Controls.Add(this.btnReplace);
			this.Controls.Add(this.btnCancel);
			this.MaximizeBox = false;
			this.MinimizeBox = false;
			this.Name = "FindReplaceForm";
			this.ShowIcon = false;
			this.ShowInTaskbar = false;
			this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
			this.Text = "Find ...";
			this.Deactivate += new System.EventHandler(this.FindReplaceForm_Deactivate);
			this.Activated += new System.EventHandler(this.FindReplaceForm_Activated);
			this.KeyUp += new System.Windows.Forms.KeyEventHandler(this.FindReplaceForm_KeyUp);
			this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.FindReplaceForm_FormClosing);
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Button btnCancel;
		private System.Windows.Forms.Button btnReplace;
		private System.Windows.Forms.Button btnFind;
		private System.Windows.Forms.TextBox txtFind;
		private System.Windows.Forms.Label lblFind;
		private System.Windows.Forms.Label lblReplace;
		private System.Windows.Forms.TextBox txtReplace;
	}
}