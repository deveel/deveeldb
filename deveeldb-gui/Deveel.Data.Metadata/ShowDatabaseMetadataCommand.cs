using System;

using Deveel.Data.Commands;

using WeifenLuo.WinFormsUI.Docking;

namespace Deveel.Data.Metadata {
	[Command("ShowMetadata", "Show Database Metadata")]
	public sealed class ShowDatabaseMetadataCommand : Command {
		#region Overrides of Command

		public override void Execute() {
			DockContent provider = Services.Resolve(typeof(IDbMetadataProvider)) as DockContent;
			if (provider != null) {
				HostWindow.ShowMetadata(provider as IDbMetadataProvider, DockState.DockLeft);
				provider.Activate();
			}
		}

		#endregion
	}
}