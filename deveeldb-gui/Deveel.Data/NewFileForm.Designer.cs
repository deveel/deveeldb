namespace Deveel.Data {
	partial class NewFileForm {
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
			this.lstFileTypes = new System.Windows.Forms.ListBox();
			this.btnOk = new System.Windows.Forms.Button();
			this.btnCancel = new System.Windows.Forms.Button();
			this.SuspendLayout();
			// 
			// lstFileTypes
			// 
			this.lstFileTypes.FormattingEnabled = true;
			this.lstFileTypes.Location = new System.Drawing.Point(12, 12);
			this.lstFileTypes.Name = "lstFileTypes";
			this.lstFileTypes.Size = new System.Drawing.Size(343, 212);
			this.lstFileTypes.TabIndex = 0;
			this.lstFileTypes.DoubleClick += new System.EventHandler(this.lstFileTypes_DoubleClick);
			this.lstFileTypes.SelectedValueChanged += new System.EventHandler(this.lstFileTypes_SelectedValueChanged);
			// 
			// btnOk
			// 
			this.btnOk.Location = new System.Drawing.Point(280, 229);
			this.btnOk.Name = "btnOk";
			this.btnOk.Size = new System.Drawing.Size(75, 23);
			this.btnOk.TabIndex = 1;
			this.btnOk.Text = "&OK";
			this.btnOk.UseVisualStyleBackColor = true;
			this.btnOk.Click += new System.EventHandler(this.btnOk_Click);
			// 
			// btnCancel
			// 
			this.btnCancel.Location = new System.Drawing.Point(199, 229);
			this.btnCancel.Name = "btnCancel";
			this.btnCancel.Size = new System.Drawing.Size(75, 23);
			this.btnCancel.TabIndex = 2;
			this.btnCancel.Text = "&Cancel";
			this.btnCancel.UseVisualStyleBackColor = true;
			this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
			// 
			// NewFileForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(367, 264);
			this.Controls.Add(this.btnCancel);
			this.Controls.Add(this.btnOk);
			this.Controls.Add(this.lstFileTypes);
			this.Name = "NewFileForm";
			this.Text = "NewFileForm";
			this.Load += new System.EventHandler(this.NewFileForm_Load);
			this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.ListBox lstFileTypes;
		private System.Windows.Forms.Button btnOk;
		private System.Windows.Forms.Button btnCancel;
	}
}