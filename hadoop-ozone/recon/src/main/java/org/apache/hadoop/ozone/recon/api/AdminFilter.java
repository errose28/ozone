package org.apache.hadoop.ozone.recon.api;


import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.Collection;

public class AdminFilter implements Filter {
  @Context
  private ResourceInfo resourceInfo;

  @Context
  private SecurityContext securityContext;

  @Inject
  private OzoneConfiguration conf;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException { }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
                       FilterChain chain) throws IOException, ServletException {
    String name = securityContext.getUserPrincipal().getName();
    Collection<String> admins = conf
        .getTrimmedStringCollection(OzoneConfigKeys.OZONE_ADMINISTRATORS);

    if (!admins.contains(name) &&
        resourceInfo.getResourceClass().isAnnotationPresent(AdminOnly.class)) {
      ((HttpServletResponse) response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    } else {
      chain.doFilter(request, response);
    }
  }

  @Override
  public void destroy() { }
}

