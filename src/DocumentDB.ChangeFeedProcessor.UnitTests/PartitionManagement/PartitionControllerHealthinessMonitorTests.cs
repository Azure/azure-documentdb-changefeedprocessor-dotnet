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
            var monitor = new Mock<IHealthMonitor>();
            var sut = new HealthMonitoringPartitionControllerDecorator(Mock.Of<IPartitionController>(), monitor.Object);
            var lease = Mock.Of<ILease>();
            await sut.AddOrUpdateLeaseAsync(lease);

            monitor.Verify(m => m.InspectAsync(It.Is<HealthMonitoringRecord>(r => r.Severity == HealthSeverity.Informational && r.Lease == lease && r.Operation == MonitoredOperation.AcquireLease && r.Exception == null)));
        }

        [Fact]
        public async Task AcquireLease_ShouldReportFailure_IfSystemIssue()
        {
            var lease = Mock.Of<ILease>();
            var monitor = new Mock<IHealthMonitor>();
            var controller = new Mock<IPartitionController>();

            Exception exception = new InvalidOperationException();
            controller
                .Setup(c => c.AddOrUpdateLeaseAsync(lease))
                .Returns(Task.FromException(exception));

            var sut = new HealthMonitoringPartitionControllerDecorator(controller.Object, monitor.Object);
            await Assert.ThrowsAsync<InvalidOperationException>(() => sut.AddOrUpdateLeaseAsync(lease));

            monitor.Verify(m => m.InspectAsync(It.Is<HealthMonitoringRecord>(r => r.Severity == HealthSeverity.Error && r.Lease == lease && r.Operation == MonitoredOperation.AcquireLease && r.Exception == exception)));
        }

    }
}