using System;

using Deveel.Data.Services;

using Moq;

using Xunit;

namespace Deveel.Data.Transactions {
	public class LockTests : IDisposable {
		private ILockable lockable;
		private Locker locker;
		private IContext context;

		public LockTests() {
			var mock = new Mock<ILockable>();
			mock.SetupGet(x => x.RefId)
				.Returns(223);

			lockable = mock.Object;

			var scope = new ServiceContainer();

			var contextMock = new Mock<IContext>();
			contextMock.SetupGet(x => x.Scope)
				.Returns(scope);
			contextMock.Setup(x => x.Dispose())
				.Callback(scope.Dispose);

			context = contextMock.Object;

			locker = new Locker(context);
		}

		[Fact]
		public void LockExclusiveReadAndEnd() {
			var handle = locker.Lock(lockable, AccessType.Read, LockingMode.Exclusive);
			Assert.True(handle.IsHandled(lockable));

			handle.Release();
			
			Assert.False(handle.IsHandled(lockable));
		}

		[Fact]
		public void ConcurrentReadInSharedMode() {
			var handle1 = locker.Lock(lockable, AccessType.Read, LockingMode.Shared);
			var handle2 = locker.Lock(lockable, AccessType.Read, LockingMode.Shared);

			handle1.WaitAll();
			handle2.WaitAll();

			handle1.Release();
			handle2.Release();
		}

		[Fact]
		public void ConcurrentReadInExclusiveMode() {
			var handle1 = locker.Lock(lockable, AccessType.Read, LockingMode.Exclusive);
			var handle2 = locker.Lock(lockable, AccessType.Write, LockingMode.Exclusive);

			handle1.WaitAll();

			Assert.Throws<LockTimeoutException>(() => handle2.WaitAll());

			handle1.Release();
			handle2.Release();
		}

		[Fact]
		public void ConcurrentReadInExclusiveMode_WaitWrite() {
			var handle1 = locker.Lock(lockable, AccessType.Read, LockingMode.Exclusive);
			var handle2 = locker.Lock(lockable, AccessType.Write, LockingMode.Exclusive);

			handle1.WaitAll();

			Assert.Throws<LockTimeoutException>(() => handle2.Wait(lockable, AccessType.Write));

			handle1.Release();
			handle2.Release();
		}


		[Fact]
		public void LockableIsLocked() {
			var handle1 = locker.Lock(lockable, AccessType.Read, LockingMode.Exclusive);
			var handle2 = locker.Lock(lockable, AccessType.Write, LockingMode.Exclusive);

			Assert.True(locker.IsLocked(lockable));

			handle1.Release();

			Assert.True(locker.IsLocked(lockable));

			handle2.Release();

			Assert.False(locker.IsLocked(lockable));
		}

		[Fact]
		public void DisposeUnreleasedHandle() {
			var handle = locker.Lock(lockable, AccessType.Read, LockingMode.Exclusive);

			Assert.True(locker.IsLocked(lockable));

			handle.Dispose();

			Assert.False(locker.IsLocked(lockable));
		}

		public void Dispose() {
			locker.Dispose();
			context.Dispose();
		}
	}
}