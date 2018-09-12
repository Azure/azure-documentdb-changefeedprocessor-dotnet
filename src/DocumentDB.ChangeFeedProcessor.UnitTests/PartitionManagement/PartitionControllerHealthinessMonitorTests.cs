using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    [Trait("Category", "Gated")]
    public class PartitionControllerHealthinessMonitorTests
    {
        [Fact]
        public async Task AcquireLease_ShouldReportHealthy_IfNoIssues()
        {
            var monitor = new Mock<IHealthinessMonitor>();
            var sut = new PartitionControllerHealthinessMonitor(Mock.Of<IPartitionController>(), monitor.Object);
            var lease = Mock.Of<ILease>();
            await sut.AddOrUpdateLeaseAsync(lease);

            monitor.Verify(m => m.InspectAsync(HealthEventLevel.Health, HealthEventPhase.AquireLease, lease, null));
        }

        [Fact]
        public async Task AcquireLease_ShouldReportFailure_IfSystemIssue()
        {
            var lease = Mock.Of<ILease>();
            var monitor = new Mock<IHealthinessMonitor>();
            var controller = new Mock<IPartitionController>();

            Exception exception = new InvalidOperationException();
            controller
                .Setup(c => c.AddOrUpdateLeaseAsync(lease))
                .Returns(Task.FromException(exception));

            var sut = new PartitionControllerHealthinessMonitor(controller.Object, monitor.Object);
            await Assert.ThrowsAsync<InvalidOperationException>(() => sut.AddOrUpdateLeaseAsync(lease));

            monitor.Verify(m => m.InspectAsync(HealthEventLevel.Error, HealthEventPhase.AquireLease, lease, exception));
        }

    }
}