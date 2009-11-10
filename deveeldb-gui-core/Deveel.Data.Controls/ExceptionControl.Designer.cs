namespace Deveel.Data.Deveel.Data.Controls {
	partial class ExceptionControl {
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

		#region Component Designer generated code

		/// <summary> 
		/// Required method for Designer support - do not modify 
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent() {
			this.lblMessage = new System.Windows.Forms.Label();
			this.txtMessage = new System.Windows.Forms.TextBox();
			this.lblStackTrace = new System.Windows.Forms.Label();
			this.txtStackTrace = new System.Windows.Forms.TextBox();
			this.SuspendLayout();
			// 
			// lblMessage
			// 
			this.lblMessage.AutoSize = true;
			this.lblMessage.Location = new System.Drawing.Point(13, 9);
			this.lblMessage.Name = "lblMessage";
			this.lblMessage.Size = new System.Drawing.Size(50, 13);
			this.lblMessage.TabIndex = 0;
			this.lblMessage.Text = "Message";
			// 
			// txtMessage
			// 
			this.txtMessage.Location = new System.Drawing.Point(16, 25);
			this.txtMessage.Multiline = true;
			this.txtMessage.Name = "txtMessage";
			this.txtMessage.ReadOnly = true;
			this.txtMessage.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			this.txtMessage.Size = new System.Drawing.Size(394, 57);
			this.txtMessage.TabIndex = 1;
			// 
			// lblStackTrace
			// 
			this.lblStackTrace.AutoSize = true;
			this.lblStackTrace.Location = new System.Drawing.Point(13, 99);
			this.lblStackTrace.Name = "lblStackTrace";
			this.lblStackTrace.Size = new System.Drawing.Size(66, 13);
			this.lblStackTrace.TabIndex = 2;
			this.lblStackTrace.Text = "Stack Trace";
			// 
			// txtStackTrace
			// 
			this.txtStackTrace.Location = new System.Drawing.Point(16, 115);
			this.txtStackTrace.Multiline = true;
			this.txtStackTrace.Name = "txtStackTrace";
			this.txtStackTrace.ReadOnly = true;
			this.txtStackTrace.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			this.txtStackTrace.Size = new System.Drawing.Size(394, 227);
			this.txtStackTrace.TabIndex = 3;
			// 
			// ExceptionControl
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.Controls.Add(this.txtStackTrace);
			this.Controls.Add(this.lblStackTrace);
			this.Controls.Add(this.txtMessage);
			this.Controls.Add(this.lblMessage);
			this.Name = "ExceptionControl";
			this.Size = new System.Drawing.Size(421, 392);
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Label lblMessage;
		private System.Windows.Forms.TextBox txtMessage;
		private System.Windows.Forms.Label lblStackTrace;
		private System.Windows.Forms.TextBox txtStackTrace;
	}
}
