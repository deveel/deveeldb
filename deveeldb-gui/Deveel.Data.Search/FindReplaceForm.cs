using System;
using System.Media;
using System.Windows.Forms;

namespace Deveel.Data.Search {
	public partial class FindReplaceForm : Form, IFindReplaceWindow {
		public FindReplaceForm(IApplicationServices services) {
			InitializeComponent();
			this.services = services;
		}

		private IApplicationServices services;

		#region Implementation of IFindReplaceWindow

		public string SearchText {
			get { return txtFind.Text; }
			set { txtFind.Text = value; }
		}

		public string ReplaceText {
			get { return txtReplace.Text; }
			set { txtReplace.Text = value; }
		}

		#endregion

		private void FindReplaceForm_KeyUp(object sender, KeyEventArgs e) {
			if (e.KeyCode == Keys.Escape) {
				e.Handled = true;
				Hide();
			}
		}

		private void FindReplaceForm_FormClosing(object sender, FormClosingEventArgs e) {
			if (e.CloseReason == CloseReason.UserClosing) {
				e.Cancel = true;
				Hide();
			}
		}

		private void btnFind_Click(object sender, EventArgs e) {
			ITextSearchProvider provider = services.HostWindow.ActiveChild as ITextSearchProvider;

			if (provider == null) {
				SystemSounds.Beep.Play();
			} else {
				HandleFindNext(provider);
			}
		}

		private void btnReplace_Click(object sender, EventArgs e) {
			ITextSearchProvider provider = services.HostWindow.ActiveChild as ITextSearchProvider;

			if (provider == null) {
				SystemSounds.Beep.Play();
			} else {
				HandleReplaceNext(provider);
			}
		}

		private void HandleFindNext(ITextSearchProvider provider) {
			int key = provider.GetHashCode();
			TextSearch search;

			if (SearchTable.Searches.ContainsKey(key)) {
				search = SearchTable.Searches[key];
				search = new TextSearch(search.Provider, search.Text);
				search.Position = provider.CursorOffset;
				SearchTable.Searches[key] = search;
			} else {
				search = new TextSearch(provider, SearchText);
				SearchTable.Searches[key] = search;
			}

			services.CommandHandler.GetCommand(typeof(FindNextCommand)).Execute();
		}

		private void HandleReplaceNext(ITextSearchProvider provider) {
			int key = provider.GetHashCode();
			TextSearch search;

			if (SearchTable.Searches.ContainsKey(key)) {
				search = SearchTable.Searches[key];
				search = new ReplaceTextSearch(provider, search.Text, ReplaceText);
				search.Position = provider.CursorOffset;
				SearchTable.Searches[key] = search;
			} else {
				search = new ReplaceTextSearch(provider, SearchText, ReplaceText);
				SearchTable.Searches[key] = search;
			}

			services.CommandHandler.GetCommand(typeof(ReplaceTextCommand)).Execute();
		}

		private void UnDimForm() {
			Opacity = 1.0;
		}

		private void DimForm() {
			Opacity = 0.8;
		}

		private void txtFind_Enter(object sender, EventArgs e) {
			UnDimForm();
		}

		private void txtFind_Leave(object sender, EventArgs e) {
			DimForm();
		}

		private void FindReplaceForm_Activated(object sender, EventArgs e) {
			if (txtFind.Focused | txtReplace.Focused) {
				UnDimForm();
			} else {
				DimForm();
			}
		}

		private void FindReplaceForm_Deactivate(object sender, EventArgs e) {
			DimForm();
		}
	}
}
