package com.flipkart.fdp.superbi.web;

import com.flipkart.fdp.superbi.web.configurations.SuperBiWebServiceConfiguration;
import com.google.inject.Module;
import java.util.List;

/**
 * Created by : waghmode.tayappa
 * Date : Jun 20, 2019
 */
public class SuperBiWebModuleProvider  {

  public List<? extends Module> getModules(SuperBiWebServiceConfiguration configService) {
    return new SuperBiWebModule(configService).getModules();
  }
}
