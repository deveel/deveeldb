namespace Deveel.Data {
	partial class CopyDialog {
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
			this.chkIncludeHeaders = new System.Windows.Forms.CheckBox();
			this.grpSeparator = new System.Windows.Forms.GroupBox();
			this.rdbCsv = new System.Windows.Forms.RadioButton();
			this.rdbTab = new System.Windows.Forms.RadioButton();
			this.rdbOther = new System.Windows.Forms.RadioButton();
			this.txtOther = new System.Windows.Forms.TextBox();
			this.btnCancel = new System.Windows.Forms.Button();
			this.btnOk = new System.Windows.Forms.Button();
			this.grpSeparator.SuspendLayout();
			this.SuspendLayout();
			// 
			// chkIncludeHeaders
			// 
			this.chkIncludeHeaders.AutoSize = true;
			this.chkIncludeHeaders.Location = new System.Drawing.Point(12, 12);
			this.chkIncludeHeaders.Name = "chkIncludeHeaders";
			this.chkIncludeHeaders.Size = new System.Drawing.Size(142, 17);
			this.chkIncludeHeaders.TabIndex = 0;
			this.chkIncludeHeaders.Text = "Include Column Headers";
			this.chkIncludeHeaders.UseVisualStyleBackColor = true;
			// 
			// grpSeparator
			// 
			this.grpSeparator.Controls.Add(this.txtOther);
			this.grpSeparator.Controls.Add(this.rdbOther);
			this.grpSeparator.Controls.Add(this.rdbTab);
			this.grpSeparator.Controls.Add(this.rdbCsv);
			this.grpSeparator.Location = new System.Drawing.Point(12, 35);
			this.grpSeparator.Name = "grpSeparator";
			this.grpSeparator.Size = new System.Drawing.Size(156, 104);
			this.grpSeparator.TabIndex = 1;
			this.grpSeparator.TabStop = false;
			this.grpSeparator.Text = "Separator";
			// 
			// rdbCsv
			// 
			this.rdbCsv.AutoSize = true;
			this.rdbCsv.Location = new System.Drawing.Point(15, 29);
			this.rdbCsv.Name = "rdbCsv";
			this.rdbCsv.Size = new System.Drawing.Size(60, 17);
			this.rdbCsv.TabIndex = 0;
			this.rdbCsv.Text = "Comma";
			this.rdbCsv.UseVisualStyleBackColor = true;
			// 
			// rdbTab
			// 
			this.rdbTab.AutoSize = true;
			this.rdbTab.Checked = true;
			this.rdbTab.Location = new System.Drawing.Point(15, 52);
			this.rdbTab.Name = "rdbTab";
			this.rdbTab.Size = new System.Drawing.Size(95, 17);
			this.rdbTab.TabIndex = 1;
			this.rdbTab.TabStop = true;
			this.rdbTab.Text = "TAB Character";
			this.rdbTab.UseVisualStyleBackColor = true;
			// 
			// rdbOther
			// 
			this.rdbOther.AutoSize = true;
			this.rdbOther.Location = new System.Drawing.Point(15, 75);
			this.rdbOther.Name = "rdbOther";
			this.rdbOther.Size = new System.Drawing.Size(51, 17);
			this.rdbOther.TabIndex = 2;
			this.rdbOther.TabStop = true;
			this.rdbOther.Text = "Other";
			this.rdbOther.UseVisualStyleBackColor = true;
			// 
			// txtOther
			// 
			this.txtOther.Location = new System.Drawing.Point(72, 75);
			this.txtOther.Name = "txtOther";
			this.txtOther.Size = new System.Drawing.Size(67, 20);
			this.txtOther.TabIndex = 3;
			// 
			// btnCancel
			// 
			this.btnCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
			this.btnCancel.Location = new System.Drawing.Point(12, 145);
			this.btnCancel.Name = "btnCancel";
			this.btnCancel.Size = new System.Drawing.Size(75, 23);
			this.btnCancel.TabIndex = 2;
			this.btnCancel.Text = "&Cancel";
			this.btnCancel.UseVisualStyleBackColor = true;
			// 
			// btnOk
			// 
			this.btnOk.Location = new System.Drawing.Point(93, 145);
			this.btnOk.Name = "btnOk";
			this.btnOk.Size = new System.Drawing.Size(75, 23);
			this.btnOk.TabIndex = 3;
			this.btnOk.Text = "&Ok";
			this.btnOk.UseVisualStyleBackColor = true;
			// 
			// CopyDialog
			// 
			this.AcceptButton = this.btnOk;
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.CancelButton = this.btnCancel;
			this.ClientSize = new System.Drawing.Size(176, 178);
			this.Controls.Add(this.btnOk);
			this.Controls.Add(this.btnCancel);
			this.Controls.Add(this.grpSeparator);
			this.Controls.Add(this.chkIncludeHeaders);
			this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
			this.MaximizeBox = false;
			this.MinimizeBox = false;
			this.Name = "CopyDialog";
			this.ShowIcon = false;
			this.ShowInTaskbar = false;
			this.SizeGripStyle = System.Windows.Forms.SizeGripStyle.Hide;
			this.Text = "Copy...";
			this.grpSeparator.ResumeLayout(false);
			this.grpSeparator.PerformLayout();
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.CheckBox chkIncludeHeaders;
		private System.Windows.Forms.GroupBox grpSeparator;
		private System.Windows.Forms.RadioButton rdbOther;
		private System.Windows.Forms.RadioButton rdbTab;
		private System.Windows.Forms.RadioButton rdbCsv;
		private System.Windows.Forms.TextBox txtOther;
		private System.Windows.Forms.Button btnCancel;
		private System.Windows.Forms.Button btnOk;
	}
}