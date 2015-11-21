using System;

using Deveel.Data.Services;

namespace Deveel.Data {
	public class Block : IBlock, IBlockParent {
		internal Block(IBlockParent parent) {
			Context = parent.CreateBlockContext();
			Context.RegisterInstance<IBlock>(this);

			Parent = parent as IBlock;
		}

		~Block() {
			Dispose(false);
		}

		public IBlock Parent { get; private set; }

		public void Dispose() {
			
		}

		protected void Dispose(bool disposing) {
			if (disposing) {
				if (Context != null)
					Context.Dispose();
			}

			Context = null;
		}

		public IBlockContext Context { get; private set; }

		IBlockContext IBlockParent.CreateBlockContext() {
			return Context.CreateBlockContext();
		}

		public virtual IBlock CreateBlock() {
			return new Block(this);
		}
	}
}
