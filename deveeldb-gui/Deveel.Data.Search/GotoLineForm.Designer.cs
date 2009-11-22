namespace Deveel.Data.Search {
	partial class GotoLineForm {
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
			this.btnGo = new System.Windows.Forms.Button();
			this.btnCancel = new System.Windows.Forms.Button();
			this.txtLineNumber = new System.Windows.Forms.TextBox();
			this.lblLineNumber = new System.Windows.Forms.Label();
			this.SuspendLayout();
			// 
			// btnGo
			// 
			this.btnGo.Location = new System.Drawing.Point(153, 51);
			this.btnGo.Name = "btnGo";
			this.btnGo.Size = new System.Drawing.Size(75, 23);
			this.btnGo.TabIndex = 0;
			this.btnGo.Text = "&Go";
			this.btnGo.UseVisualStyleBackColor = true;
			this.btnGo.Click += new System.EventHandler(this.btnGo_Click);
			// 
			// btnCancel
			// 
			this.btnCancel.Location = new System.Drawing.Point(72, 51);
			this.btnCancel.Name = "btnCancel";
			this.btnCancel.Size = new System.Drawing.Size(75, 23);
			this.btnCancel.TabIndex = 1;
			this.btnCancel.Text = "&Cancel";
			this.btnCancel.UseVisualStyleBackColor = true;
			// 
			// txtLineNumber
			// 
			this.txtLineNumber.Location = new System.Drawing.Point(12, 25);
			this.txtLineNumber.Name = "txtLineNumber";
			this.txtLineNumber.Size = new System.Drawing.Size(216, 20);
			this.txtLineNumber.TabIndex = 2;
			// 
			// lblLineNumber
			// 
			this.lblLineNumber.AutoSize = true;
			this.lblLineNumber.Location = new System.Drawing.Point(9, 9);
			this.lblLineNumber.Name = "lblLineNumber";
			this.lblLineNumber.Size = new System.Drawing.Size(65, 13);
			this.lblLineNumber.TabIndex = 3;
			this.lblLineNumber.Text = "&Line number";
			// 
			// GotoLineForm
			// 
			this.AcceptButton = this.btnGo;
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.CancelButton = this.btnCancel;
			this.ClientSize = new System.Drawing.Size(240, 85);
			this.Controls.Add(this.lblLineNumber);
			this.Controls.Add(this.txtLineNumber);
			this.Controls.Add(this.btnCancel);
			this.Controls.Add(this.btnGo);
			this.MaximizeBox = false;
			this.MinimizeBox = false;
			this.Name = "GotoLineForm";
			this.ShowIcon = false;
			this.ShowInTaskbar = false;
			this.Text = "Go to line...";
			this.Load += new System.EventHandler(this.GotoLineForm_Load);
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Button btnGo;
		private System.Windows.Forms.Button btnCancel;
		private System.Windows.Forms.TextBox txtLineNumber;
		private System.Windows.Forms.Label lblLineNumber;
	}
}