namespace Deveel.Data {
	partial class OptionsForm {
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
			this.lstSettingsProviders = new System.Windows.Forms.ListBox();
			this.grpSettings = new System.Windows.Forms.GroupBox();
			this.btnOk = new System.Windows.Forms.Button();
			this.btnCancel = new System.Windows.Forms.Button();
			this.propertyGrid = new System.Windows.Forms.PropertyGrid();
			this.grpSettings.SuspendLayout();
			this.SuspendLayout();
			// 
			// lstSettingsProviders
			// 
			this.lstSettingsProviders.FormattingEnabled = true;
			this.lstSettingsProviders.Location = new System.Drawing.Point(12, 12);
			this.lstSettingsProviders.Name = "lstSettingsProviders";
			this.lstSettingsProviders.Size = new System.Drawing.Size(166, 264);
			this.lstSettingsProviders.TabIndex = 0;
			this.lstSettingsProviders.SelectedIndexChanged += new System.EventHandler(this.lstSettingsProviders_SelectedIndexChanged);
			// 
			// grpSettings
			// 
			this.grpSettings.Controls.Add(this.propertyGrid);
			this.grpSettings.Location = new System.Drawing.Point(184, 12);
			this.grpSettings.Name = "grpSettings";
			this.grpSettings.Size = new System.Drawing.Size(391, 269);
			this.grpSettings.TabIndex = 1;
			this.grpSettings.TabStop = false;
			this.grpSettings.Text = "Settings";
			// 
			// btnOk
			// 
			this.btnOk.Location = new System.Drawing.Point(500, 298);
			this.btnOk.Name = "btnOk";
			this.btnOk.Size = new System.Drawing.Size(75, 23);
			this.btnOk.TabIndex = 2;
			this.btnOk.Text = "&OK";
			this.btnOk.UseVisualStyleBackColor = true;
			this.btnOk.Click += new System.EventHandler(this.btnOk_Click);
			// 
			// btnCancel
			// 
			this.btnCancel.Location = new System.Drawing.Point(419, 298);
			this.btnCancel.Name = "btnCancel";
			this.btnCancel.Size = new System.Drawing.Size(75, 23);
			this.btnCancel.TabIndex = 3;
			this.btnCancel.Text = "&Cancel";
			this.btnCancel.UseVisualStyleBackColor = true;
			this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
			// 
			// propertyGrid
			// 
			this.propertyGrid.Dock = System.Windows.Forms.DockStyle.Fill;
			this.propertyGrid.Location = new System.Drawing.Point(3, 16);
			this.propertyGrid.Name = "propertyGrid";
			this.propertyGrid.Size = new System.Drawing.Size(385, 250);
			this.propertyGrid.TabIndex = 0;
			// 
			// OptionsForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(587, 333);
			this.Controls.Add(this.btnCancel);
			this.Controls.Add(this.btnOk);
			this.Controls.Add(this.grpSettings);
			this.Controls.Add(this.lstSettingsProviders);
			this.Name = "OptionsForm";
			this.Text = "Options";
			this.Load += new System.EventHandler(this.OptionsForm_Load);
			this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.OptionsForm_FormClosing);
			this.grpSettings.ResumeLayout(false);
			this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.ListBox lstSettingsProviders;
		private System.Windows.Forms.GroupBox grpSettings;
		private System.Windows.Forms.Button btnOk;
		private System.Windows.Forms.Button btnCancel;
		private System.Windows.Forms.PropertyGrid propertyGrid;
	}
}