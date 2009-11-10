using System;
using System.Drawing;
using System.Windows.Forms;

using SWFControl = System.Windows.Forms.Control;

namespace Deveel.Data.Commands {
	public sealed class CommandControlBuilder {
		public CommandControlBuilder(CommandHandler handler) {
			this.handler = handler;
		}

		private readonly CommandHandler handler;

		[System.Diagnostics.DebuggerNonUserCode]
		private static void CommandItemClick(object sender, EventArgs e) {
			ToolStripItem item = sender as ToolStripItem;

			if (item != null) {
				ICommand cmd = item.Tag as ICommand;

				if (cmd != null) {
					cmd.Execute();
				}
			}
		}

		private static void LinkLabelLinkClicked(object sender, LinkLabelLinkClickedEventArgs e) {
			SWFControl linkLabel = sender as SWFControl;

			if (linkLabel != null) {
				ICommand command = linkLabel.Tag as ICommand;

				if (command != null)
					command.Execute();
			}
		}

		[System.Diagnostics.DebuggerNonUserCode]
		private static void TopLevelMenuDropDownOpening(object sender, EventArgs e) {
			ToolStripMenuItem menuItem = sender as ToolStripMenuItem;

			if (menuItem == null)
				return;

			foreach (ToolStripItem item in menuItem.DropDownItems) {
				ICommand command = item.Tag as ICommand;

				if (command != null)
					item.Enabled = command.Enabled;
			}
		}

		[System.Diagnostics.DebuggerNonUserCode]
		private static void TopLevelMenuDropDownClosed(object sender, EventArgs e) {
			ToolStripMenuItem menuItem = sender as ToolStripMenuItem;

			if (menuItem == null)
				return;

			foreach (ToolStripItem item in menuItem.DropDownItems) {
				ICommand command = item.Tag as ICommand;

				if (command != null)
					item.Enabled = true;
			}
		}


		public ToolStripButton CreateToolStripButton(Type commandType) {
			if (!typeof(ICommand).IsAssignableFrom(commandType))
				throw new ArgumentException();

			ToolStripButton button = new ToolStripButton();
			ICommand cmd = handler.GetCommand(commandType);

			button.DisplayStyle = ToolStripItemDisplayStyle.Image;
			button.Image = cmd.SmallImage;
			button.ImageTransparentColor = Color.Magenta;
			button.Name = cmd.GetType().Name + "ToolStripButton";
			button.Tag = cmd;
			button.Text = cmd.Name;
			button.Click += CommandItemClick;

			return button;
		}

		public ToolStripMenuItem CreateToolStripMenuItem(Type commandType) {
			if (!typeof(ICommand).IsAssignableFrom(commandType))
				throw new ArgumentException();

			ToolStripMenuItem menuItem = new ToolStripMenuItem();
			ICommand cmd = handler.GetCommand(commandType);

			menuItem.Name = cmd.GetType().Name + "ToolStripMenuItem";
			menuItem.Text = cmd.Name;
			menuItem.Tag = cmd;
			menuItem.ShortcutKeys = cmd.Shortcut;
			menuItem.Image = cmd.SmallImage;
			menuItem.Click += CommandItemClick;

			return menuItem;
		}

		public LinkLabel CreateLinkLabel(Type commandType) {
			if (!typeof(ICommand).IsAssignableFrom(commandType))
				throw new ArgumentException();

			LinkLabel linkLabel = new LinkLabel();
			ICommand cmd = handler.GetCommand(commandType);

			linkLabel.AutoSize = true;
			linkLabel.Name = cmd.GetType().Name + "LinkLabel";
			linkLabel.TabStop = true;
			linkLabel.Text = cmd.Name.Replace("&", string.Empty);
			linkLabel.Tag = cmd;
			linkLabel.Padding = new Padding(4);
			linkLabel.LinkClicked += LinkLabelLinkClicked;

			return linkLabel;
		}

		public static void MonitorMenuItemsOpeningForEnabling(ToolStrip menuStrip) {
			if (menuStrip is ContextMenuStrip || menuStrip is MenuStrip) {
				foreach (ToolStripItem item in menuStrip.Items) {
					ToolStripMenuItem topLevelMenu = item as ToolStripMenuItem;
					if (topLevelMenu != null) {
						topLevelMenu.DropDownOpening += TopLevelMenuDropDownOpening;
						topLevelMenu.DropDownClosed += TopLevelMenuDropDownClosed;
					}
				}
			}
		}
	}
}