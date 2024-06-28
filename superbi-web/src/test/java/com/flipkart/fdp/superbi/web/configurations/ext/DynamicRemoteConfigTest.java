package com.flipkart.fdp.superbi.web.configurations.ext;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.flipkart.fdp.superbi.web.configurations.RemoteApplicationConfig;
import com.flipkart.fdp.utils.cfg.v2.DefaultConfigService;
import java.util.function.UnaryOperator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by: nitin.chauhan1 on 06/03/24.
 */
@RunWith(PowerMockRunner.class)
public class DynamicRemoteConfigTest {

  @Mock
  private DefaultConfigService mockConfigService;

  @Mock
  private RemoteApplicationConfig applicationConfig;

  @Before
  public void setUp() {
    doNothing().when(mockConfigService).setValueSubstitutor(any(UnaryOperator.class));
    doNothing().when(applicationConfig).setConfigService(any(DefaultConfigService.class));
  }

  @Test
  public void testOnUpdateFailedConfigBuild() {
    DynamicRemoteConfig dynamicRemoteConfig = new DynamicRemoteConfig(applicationConfig);
    when(applicationConfig.build()).thenReturn(false);

    assertFalse(dynamicRemoteConfig.onUpdate(mockConfigService));
  }

  @Test
  public void testOnUpdateSuccessfulConfigBuild() {
    DynamicRemoteConfig dynamicRemoteConfig = new DynamicRemoteConfig(applicationConfig);
    when(applicationConfig.build()).thenReturn(true);

    assertTrue(dynamicRemoteConfig.onUpdate(mockConfigService));
  }
}