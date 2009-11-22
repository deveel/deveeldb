namespace Deveel.Data.ConnectionStrings {
	partial class ConnectionStringForm {
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
			System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ConnectionStringForm));
			this.connsTreeView = new System.Windows.Forms.TreeView();
			this.tabParameters = new System.Windows.Forms.TabControl();
			this.tabConnParams = new System.Windows.Forms.TabPage();
			this.lblComment = new System.Windows.Forms.Label();
			this.txtComment = new System.Windows.Forms.TextBox();
			this.txtSchema = new System.Windows.Forms.TextBox();
			this.lblSchema = new System.Windows.Forms.Label();
			this.txtPort = new System.Windows.Forms.TextBox();
			this.lblPort = new System.Windows.Forms.Label();
			this.lblDatabase = new System.Windows.Forms.Label();
			this.txtDatabase = new System.Windows.Forms.TextBox();
			this.lblHost = new System.Windows.Forms.Label();
			this.txtHost = new System.Windows.Forms.TextBox();
			this.lblPass = new System.Windows.Forms.Label();
			this.txtPass = new System.Windows.Forms.TextBox();
			this.txtUser = new System.Windows.Forms.TextBox();
			this.lblUser = new System.Windows.Forms.Label();
			this.lblName = new System.Windows.Forms.Label();
			this.txtConnName = new System.Windows.Forms.TextBox();
			this.tabAdvConnParams = new System.Windows.Forms.TabPage();
			this.propsGridView = new System.Windows.Forms.DataGridView();
			this.btnDelete = new System.Windows.Forms.Button();
			this.btnDuplicate = new System.Windows.Forms.Button();
			this.btnNew = new System.Windows.Forms.Button();
			this.btnTest = new System.Windows.Forms.Button();
			this.btnClose = new System.Windows.Forms.Button();
			this.btnDiscard = new System.Windows.Forms.Button();
			this.btnApply = new System.Windows.Forms.Button();
			this.treeImageList = new System.Windows.Forms.ImageList(this.components);
			this.colSettingName = new System.Windows.Forms.DataGridViewComboBoxColumn();
			this.colSettingValue = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.tabParameters.SuspendLayout();
			this.tabConnParams.SuspendLayout();
			this.tabAdvConnParams.SuspendLayout();
			((System.ComponentModel.ISupportInitialize)(this.propsGridView)).BeginInit();
			this.SuspendLayout();
			// 
			// connsTreeView
			// 
			this.connsTreeView.ImageIndex = 0;
			this.connsTreeView.ImageList = this.treeImageList;
			this.connsTreeView.Location = new System.Drawing.Point(12, 12);
			this.connsTreeView.Name = "connsTreeView";
			this.connsTreeView.SelectedImageIndex = 0;
			this.connsTreeView.Size = new System.Drawing.Size(247, 332);
			this.connsTreeView.TabIndex = 0;
			this.connsTreeView.AfterSelect += new System.Windows.Forms.TreeViewEventHandler(this.connsTreeView_AfterSelect);
			this.connsTreeView.BeforeSelect += new System.Windows.Forms.TreeViewCancelEventHandler(this.connsTreeView_BeforeSelect);
			// 
			// tabParameters
			// 
			this.tabParameters.Controls.Add(this.tabConnParams);
			this.tabParameters.Controls.Add(this.tabAdvConnParams);
			this.tabParameters.Enabled = false;
			this.tabParameters.Location = new System.Drawing.Point(265, 12);
			this.tabParameters.Name = "tabParameters";
			this.tabParameters.SelectedIndex = 0;
			this.tabParameters.Size = new System.Drawing.Size(423, 303);
			this.tabParameters.TabIndex = 3;
			// 
			// tabConnParams
			// 
			this.tabConnParams.Controls.Add(this.lblComment);
			this.tabConnParams.Controls.Add(this.txtComment);
			this.tabConnParams.Controls.Add(this.txtSchema);
			this.tabConnParams.Controls.Add(this.lblSchema);
			this.tabConnParams.Controls.Add(this.txtPort);
			this.tabConnParams.Controls.Add(this.lblPort);
			this.tabConnParams.Controls.Add(this.lblDatabase);
			this.tabConnParams.Controls.Add(this.txtDatabase);
			this.tabConnParams.Controls.Add(this.lblHost);
			this.tabConnParams.Controls.Add(this.txtHost);
			this.tabConnParams.Controls.Add(this.lblPass);
			this.tabConnParams.Controls.Add(this.txtPass);
			this.tabConnParams.Controls.Add(this.txtUser);
			this.tabConnParams.Controls.Add(this.lblUser);
			this.tabConnParams.Controls.Add(this.lblName);
			this.tabConnParams.Controls.Add(this.txtConnName);
			this.tabConnParams.Location = new System.Drawing.Point(4, 22);
			this.tabConnParams.Name = "tabConnParams";
			this.tabConnParams.Padding = new System.Windows.Forms.Padding(3);
			this.tabConnParams.Size = new System.Drawing.Size(415, 277);
			this.tabConnParams.TabIndex = 0;
			this.tabConnParams.Text = "Parameters";
			this.tabConnParams.UseVisualStyleBackColor = true;
			// 
			// lblComment
			// 
			this.lblComment.AutoSize = true;
			this.lblComment.Location = new System.Drawing.Point(20, 204);
			this.lblComment.Name = "lblComment";
			this.lblComment.Size = new System.Drawing.Size(51, 13);
			this.lblComment.TabIndex = 15;
			this.lblComment.Text = "Comment";
			// 
			// txtComment
			// 
			this.txtComment.Location = new System.Drawing.Point(148, 201);
			this.txtComment.Multiline = true;
			this.txtComment.Name = "txtComment";
			this.txtComment.Size = new System.Drawing.Size(221, 53);
			this.txtComment.TabIndex = 14;
			// 
			// txtSchema
			// 
			this.txtSchema.Location = new System.Drawing.Point(148, 175);
			this.txtSchema.Name = "txtSchema";
			this.txtSchema.Size = new System.Drawing.Size(221, 20);
			this.txtSchema.TabIndex = 13;
			// 
			// lblSchema
			// 
			this.lblSchema.AutoSize = true;
			this.lblSchema.Location = new System.Drawing.Point(20, 178);
			this.lblSchema.Name = "lblSchema";
			this.lblSchema.Size = new System.Drawing.Size(46, 13);
			this.lblSchema.TabIndex = 12;
			this.lblSchema.Text = "Schema";
			// 
			// txtPort
			// 
			this.txtPort.Location = new System.Drawing.Point(148, 121);
			this.txtPort.MaxLength = 4;
			this.txtPort.Name = "txtPort";
			this.txtPort.Size = new System.Drawing.Size(52, 20);
			this.txtPort.TabIndex = 11;
			// 
			// lblPort
			// 
			this.lblPort.AutoSize = true;
			this.lblPort.Location = new System.Drawing.Point(20, 124);
			this.lblPort.Name = "lblPort";
			this.lblPort.Size = new System.Drawing.Size(26, 13);
			this.lblPort.TabIndex = 10;
			this.lblPort.Text = "Port";
			// 
			// lblDatabase
			// 
			this.lblDatabase.AutoSize = true;
			this.lblDatabase.Location = new System.Drawing.Point(20, 152);
			this.lblDatabase.Name = "lblDatabase";
			this.lblDatabase.Size = new System.Drawing.Size(80, 13);
			this.lblDatabase.TabIndex = 9;
			this.lblDatabase.Text = "Initial Database";
			// 
			// txtDatabase
			// 
			this.txtDatabase.Location = new System.Drawing.Point(148, 149);
			this.txtDatabase.Name = "txtDatabase";
			this.txtDatabase.Size = new System.Drawing.Size(221, 20);
			this.txtDatabase.TabIndex = 8;
			this.txtDatabase.TextChanged += new System.EventHandler(this.txtDatabase_TextChanged);
			// 
			// lblHost
			// 
			this.lblHost.AutoSize = true;
			this.lblHost.Location = new System.Drawing.Point(20, 98);
			this.lblHost.Name = "lblHost";
			this.lblHost.Size = new System.Drawing.Size(70, 13);
			this.lblHost.TabIndex = 7;
			this.lblHost.Text = "Host Address";
			// 
			// txtHost
			// 
			this.txtHost.Location = new System.Drawing.Point(148, 95);
			this.txtHost.Name = "txtHost";
			this.txtHost.Size = new System.Drawing.Size(221, 20);
			this.txtHost.TabIndex = 6;
			this.txtHost.TextChanged += new System.EventHandler(this.txtHost_TextChanged);
			// 
			// lblPass
			// 
			this.lblPass.AutoSize = true;
			this.lblPass.Location = new System.Drawing.Point(20, 72);
			this.lblPass.Name = "lblPass";
			this.lblPass.Size = new System.Drawing.Size(53, 13);
			this.lblPass.TabIndex = 5;
			this.lblPass.Text = "Password";
			// 
			// txtPass
			// 
			this.txtPass.Location = new System.Drawing.Point(148, 69);
			this.txtPass.Name = "txtPass";
			this.txtPass.Size = new System.Drawing.Size(221, 20);
			this.txtPass.TabIndex = 4;
			this.txtPass.UseSystemPasswordChar = true;
			// 
			// txtUser
			// 
			this.txtUser.Location = new System.Drawing.Point(148, 43);
			this.txtUser.Name = "txtUser";
			this.txtUser.Size = new System.Drawing.Size(221, 20);
			this.txtUser.TabIndex = 3;
			this.txtUser.TextChanged += new System.EventHandler(this.txtUser_TextChanged);
			// 
			// lblUser
			// 
			this.lblUser.AutoSize = true;
			this.lblUser.Location = new System.Drawing.Point(20, 46);
			this.lblUser.Name = "lblUser";
			this.lblUser.Size = new System.Drawing.Size(55, 13);
			this.lblUser.TabIndex = 2;
			this.lblUser.Text = "Username";
			// 
			// lblName
			// 
			this.lblName.AutoSize = true;
			this.lblName.Location = new System.Drawing.Point(20, 19);
			this.lblName.Name = "lblName";
			this.lblName.Size = new System.Drawing.Size(92, 13);
			this.lblName.TabIndex = 1;
			this.lblName.Text = "Connection Name";
			// 
			// txtConnName
			// 
			this.txtConnName.Location = new System.Drawing.Point(148, 16);
			this.txtConnName.Name = "txtConnName";
			this.txtConnName.Size = new System.Drawing.Size(221, 20);
			this.txtConnName.TabIndex = 0;
			this.txtConnName.TextChanged += new System.EventHandler(this.txtConnName_TextChanged);
			// 
			// tabAdvConnParams
			// 
			this.tabAdvConnParams.Controls.Add(this.propsGridView);
			this.tabAdvConnParams.Location = new System.Drawing.Point(4, 22);
			this.tabAdvConnParams.Name = "tabAdvConnParams";
			this.tabAdvConnParams.Padding = new System.Windows.Forms.Padding(3);
			this.tabAdvConnParams.Size = new System.Drawing.Size(415, 277);
			this.tabAdvConnParams.TabIndex = 1;
			this.tabAdvConnParams.Text = "Advanced";
			this.tabAdvConnParams.UseVisualStyleBackColor = true;
			// 
			// propsGridView
			// 
			this.propsGridView.AllowUserToOrderColumns = true;
			this.propsGridView.AllowUserToResizeColumns = false;
			this.propsGridView.AllowUserToResizeRows = false;
			this.propsGridView.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.propsGridView.ColumnHeadersVisible = false;
			this.propsGridView.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.colSettingName,
            this.colSettingValue});
			this.propsGridView.Dock = System.Windows.Forms.DockStyle.Fill;
			this.propsGridView.Location = new System.Drawing.Point(3, 3);
			this.propsGridView.Name = "propsGridView";
			this.propsGridView.RowHeadersVisible = false;
			this.propsGridView.Size = new System.Drawing.Size(409, 271);
			this.propsGridView.TabIndex = 0;
			// 
			// btnDelete
			// 
			this.btnDelete.Enabled = false;
			this.btnDelete.Location = new System.Drawing.Point(613, 321);
			this.btnDelete.Name = "btnDelete";
			this.btnDelete.Size = new System.Drawing.Size(75, 23);
			this.btnDelete.TabIndex = 4;
			this.btnDelete.Text = "&Delete";
			this.btnDelete.UseVisualStyleBackColor = true;
			this.btnDelete.Click += new System.EventHandler(this.btnDelete_Click);
			// 
			// btnDuplicate
			// 
			this.btnDuplicate.Location = new System.Drawing.Point(532, 321);
			this.btnDuplicate.Name = "btnDuplicate";
			this.btnDuplicate.Size = new System.Drawing.Size(75, 23);
			this.btnDuplicate.TabIndex = 5;
			this.btnDuplicate.Text = "Du&licate";
			this.btnDuplicate.UseVisualStyleBackColor = true;
			this.btnDuplicate.Click += new System.EventHandler(this.btnDuplicate_Click);
			// 
			// btnNew
			// 
			this.btnNew.Location = new System.Drawing.Point(451, 321);
			this.btnNew.Name = "btnNew";
			this.btnNew.Size = new System.Drawing.Size(75, 23);
			this.btnNew.TabIndex = 6;
			this.btnNew.Text = "&New";
			this.btnNew.UseVisualStyleBackColor = true;
			this.btnNew.Click += new System.EventHandler(this.btnNew_Click);
			// 
			// btnTest
			// 
			this.btnTest.Enabled = false;
			this.btnTest.Location = new System.Drawing.Point(265, 321);
			this.btnTest.Name = "btnTest";
			this.btnTest.Size = new System.Drawing.Size(75, 23);
			this.btnTest.TabIndex = 7;
			this.btnTest.Text = "&Test";
			this.btnTest.UseVisualStyleBackColor = true;
			this.btnTest.Click += new System.EventHandler(this.btnTest_Click);
			// 
			// btnClose
			// 
			this.btnClose.Location = new System.Drawing.Point(592, 370);
			this.btnClose.Name = "btnClose";
			this.btnClose.Size = new System.Drawing.Size(96, 23);
			this.btnClose.TabIndex = 8;
			this.btnClose.Text = "&Close";
			this.btnClose.UseVisualStyleBackColor = true;
			this.btnClose.Click += new System.EventHandler(this.btnClose_Click);
			// 
			// btnDiscard
			// 
			this.btnDiscard.Enabled = false;
			this.btnDiscard.Location = new System.Drawing.Point(490, 370);
			this.btnDiscard.Name = "btnDiscard";
			this.btnDiscard.Size = new System.Drawing.Size(96, 23);
			this.btnDiscard.TabIndex = 9;
			this.btnDiscard.Text = "D&iscard";
			this.btnDiscard.UseVisualStyleBackColor = true;
			this.btnDiscard.Click += new System.EventHandler(this.btnDiscard_Click);
			// 
			// btnApply
			// 
			this.btnApply.Enabled = false;
			this.btnApply.Location = new System.Drawing.Point(388, 370);
			this.btnApply.Name = "btnApply";
			this.btnApply.Size = new System.Drawing.Size(96, 23);
			this.btnApply.TabIndex = 10;
			this.btnApply.Text = "&Apply";
			this.btnApply.UseVisualStyleBackColor = true;
			this.btnApply.Click += new System.EventHandler(this.btnApply_Click);
			// 
			// treeImageList
			// 
			this.treeImageList.ImageStream = ((System.Windows.Forms.ImageListStreamer)(resources.GetObject("treeImageList.ImageStream")));
			this.treeImageList.TransparentColor = System.Drawing.Color.Transparent;
			this.treeImageList.Images.SetKeyName(0, "database_link.png");
			this.treeImageList.Images.SetKeyName(1, "database.png");
			this.treeImageList.Images.SetKeyName(2, "server.png");
			this.treeImageList.Images.SetKeyName(3, "user.png");
			// 
			// colSettingName
			// 
			this.colSettingName.HeaderText = "Setting";
			this.colSettingName.Items.AddRange(new object[] {
            "Boot",
            "Create",
            "CreateOrBoot"});
			this.colSettingName.Name = "colSettingName";
			this.colSettingName.ReadOnly = true;
			this.colSettingName.Width = 200;
			// 
			// colSettingValue
			// 
			this.colSettingValue.HeaderText = "Value";
			this.colSettingValue.Name = "colSettingValue";
			this.colSettingValue.Width = 205;
			// 
			// ConnectionStringForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(700, 405);
			this.Controls.Add(this.btnApply);
			this.Controls.Add(this.btnDiscard);
			this.Controls.Add(this.btnClose);
			this.Controls.Add(this.btnTest);
			this.Controls.Add(this.btnNew);
			this.Controls.Add(this.btnDuplicate);
			this.Controls.Add(this.btnDelete);
			this.Controls.Add(this.tabParameters);
			this.Controls.Add(this.connsTreeView);
			this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
			this.MaximizeBox = false;
			this.MinimizeBox = false;
			this.Name = "ConnectionStringForm";
			this.ShowInTaskbar = false;
			this.Text = "Connection String Edit";
			this.Shown += new System.EventHandler(this.ConnectionStringForm_Shown);
			this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.ConnectionStringForm_FormClosing);
			this.tabParameters.ResumeLayout(false);
			this.tabConnParams.ResumeLayout(false);
			this.tabConnParams.PerformLayout();
			this.tabAdvConnParams.ResumeLayout(false);
			((System.ComponentModel.ISupportInitialize)(this.propsGridView)).EndInit();
			this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.TreeView connsTreeView;
		private System.Windows.Forms.TabControl tabParameters;
		private System.Windows.Forms.TabPage tabConnParams;
		private System.Windows.Forms.TabPage tabAdvConnParams;
		private System.Windows.Forms.Button btnDelete;
		private System.Windows.Forms.Button btnDuplicate;
		private System.Windows.Forms.Button btnNew;
		private System.Windows.Forms.Button btnTest;
		private System.Windows.Forms.Button btnClose;
		private System.Windows.Forms.Button btnDiscard;
		private System.Windows.Forms.Button btnApply;
		private System.Windows.Forms.Label lblName;
		private System.Windows.Forms.TextBox txtConnName;
		private System.Windows.Forms.TextBox txtUser;
		private System.Windows.Forms.Label lblUser;
		private System.Windows.Forms.Label lblPass;
		private System.Windows.Forms.TextBox txtPass;
		private System.Windows.Forms.Label lblSchema;
		private System.Windows.Forms.TextBox txtPort;
		private System.Windows.Forms.Label lblPort;
		private System.Windows.Forms.Label lblDatabase;
		private System.Windows.Forms.TextBox txtDatabase;
		private System.Windows.Forms.Label lblHost;
		private System.Windows.Forms.TextBox txtHost;
		private System.Windows.Forms.TextBox txtSchema;
		private System.Windows.Forms.Label lblComment;
		private System.Windows.Forms.TextBox txtComment;
		private System.Windows.Forms.DataGridView propsGridView;
		private System.Windows.Forms.ImageList treeImageList;
		private System.Windows.Forms.DataGridViewComboBoxColumn colSettingName;
		private System.Windows.Forms.DataGridViewTextBoxColumn colSettingValue;
	}
}