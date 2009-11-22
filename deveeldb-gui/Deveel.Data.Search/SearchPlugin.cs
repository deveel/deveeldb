using System;
using System.Windows.Forms;

using Deveel.Data.Commands;
using Deveel.Data.Plugins;

namespace Deveel.Data.Search {
	[Plugin("Text Search", Description = "Provides utilities to search text.", Order = 50)]
	public sealed class SearchPlugin : Plugin {
		#region Overrides of Plugin

		public override void Init() {
			Services.RegisterComponent("GoToLineForm", typeof(GotoLineForm));

			CommandControlBuilder controlBuilder = new CommandControlBuilder(Services.CommandHandler);

			ToolStripMenuItem editMenu = Services.HostWindow.GetMenuItem("Edit");

			int startIndex = CommandControlBuilder.ToolStripMenuItemIndex(editMenu, typeof(ShowOptionsCommand));

			// add the find to the edit menu before the "Options"
			editMenu.DropDownItems.Insert(startIndex, new ToolStripSeparator());
			editMenu.DropDownItems.Insert(startIndex, controlBuilder.CreateToolStripMenuItem(typeof(ShowFindTextCommand)));
			editMenu.DropDownItems.Insert(startIndex, controlBuilder.CreateToolStripMenuItem(typeof(FindNextCommand)));
			editMenu.DropDownItems.Insert(startIndex, controlBuilder.CreateToolStripMenuItem(typeof(ReplaceTextCommand)));
			editMenu.DropDownItems.Insert(startIndex, controlBuilder.CreateToolStripMenuItem(typeof(ShowGotoLineCommand)));

			ToolStripItem item = editMenu.DropDownItems["FindNextToolStripMenuItem"];
			item.Visible = false;
			item = editMenu.DropDownItems["ReplaceTextToolStripMenuItem"];
			item.Visible = false;

			Services.HostWindow.AddToolStripSeperator(-1);
			Services.HostWindow.AddToolStripCommand(-1, typeof(ShowFindTextCommand));
		}

		#endregion
	}
}