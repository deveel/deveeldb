using System;

using Deveel.Data.Services;

namespace Deveel.Data {
	public sealed class BlockContext : Context, IBlockContext {
		internal BlockContext(IContext parent)
			: base(parent) {
		}

		protected override string ContextName {
			get { return ContextNames.Block; }
		}

		public IBlockContext CreateBlockContext() {
			return new BlockContext(this);
		}
	}
}
