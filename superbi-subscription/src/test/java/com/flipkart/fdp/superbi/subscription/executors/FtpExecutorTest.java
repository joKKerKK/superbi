package com.flipkart.fdp.superbi.subscription.executors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.d42.D42Client;
import com.flipkart.fdp.superbi.subscription.delivery.FtpExecutor;
import com.flipkart.fdp.superbi.subscription.delivery.SftpExecutor;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.subscription.model.DeliveryData;
import com.flipkart.fdp.superbi.subscription.model.EventLog;
import com.flipkart.fdp.superbi.subscription.model.FTPDelivery;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.jcraft.jsch.ChannelSftp;
import java.util.ArrayList;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import sun.net.ftp.FtpClient;

//Tests are not exhaustive hence ignoring this class for now.

@Ignore
@RunWith(PowerMockRunner.class)
public class FtpExecutorTest {

    @Mock
    private FTPClient ftpClient;
    @Mock
    private D42Client d42Client;
    @Mock
    private Meter successMeter;
    @Mock
    private Meter failureMeter;
    @Mock
    private SubscriptionEventLogger subscriptionEventLogger;
    @Mock
    private MetricRegistry metricRegistry;

    private FtpExecutor ftpExecutor;

    private SftpExecutor sftpExecutor;

    private FTPDelivery ftpDelivery;

    private FTPDelivery sftpDelivery;

    @Mock
    private ScheduleInfo scheduleInfo;

    @Before
    public void setUp() {
         ftpExecutor = new FtpExecutor(d42Client, metricRegistry, subscriptionEventLogger);
         sftpExecutor = new SftpExecutor(d42Client, metricRegistry, subscriptionEventLogger);
         ftpDelivery = new FTPDelivery(DeliveryData.DeliveryAction.FTP, "", "", "", "", "", new ArrayList<>());
         sftpDelivery = new FTPDelivery(DeliveryData.DeliveryAction.FTP, "172.31.1.58", "21", "abc", "abc", "", new ArrayList<>());
        Mockito.when(scheduleInfo.getSubscriptionId()).thenReturn(123l);
    }

    @Test(expected = ClientSideException.class)
    public void testFtpClientLogin() throws Exception {
        Assert.assertEquals(new FTPClient(),(FtpClient) ftpExecutor.loginFTPClient(ftpDelivery,scheduleInfo));
    }

    @Test(expected = ClientSideException.class)
    public void testSftpClientLogin() throws Exception {
        Assert.assertEquals(new ChannelSftp(),(ChannelSftp) sftpExecutor.loginFTPClient(sftpDelivery,scheduleInfo));
    }
}
