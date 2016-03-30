using System;

namespace Deveel.Data.Sql.Triggers {
	public abstract class Trigger : IDbObject, IDisposable {
		protected Trigger(TriggerInfo triggerInfo) {
			if (triggerInfo ==null)
				throw new ArgumentNullException("triggerInfo");

			TriggerInfo = triggerInfo;
		}

		public TriggerInfo TriggerInfo { get; private set; }

		IObjectInfo IDbObject.ObjectInfo {
			get { return TriggerInfo; }
		}

		protected virtual void Dispose(bool disposing) {
			TriggerInfo = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public bool CanFire(TableEvent tableEvent) {
			return TriggerInfo.CanFire(tableEvent);
		}

		public abstract void Fire(TableEvent tableEvent, IRequest context);
	}
}
