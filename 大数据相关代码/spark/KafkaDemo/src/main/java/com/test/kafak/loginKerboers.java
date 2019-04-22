package com.test.kafak;

public class loginKerboers {
    /**
     * 以jass.conf文件认证kerberos
     */
    public static void getKerberosJaas(){
        System.setProperty("java.security.krb5.conf",
                Thread.currentThread().getContextClassLoader().getResource("krb5.conf").getPath());
        //加载本地jass.conf文件
        System.setProperty("java.security.auth.login.config",
                Thread.currentThread().getContextClassLoader().getResource("jaas.conf").getPath());
        //加载临时jass.conf
       /* File jaasConf = KerberosUtils.configureJAAS(Thread.currentThread().getContextClassLoader()
                .getResource("wms_dev.keytab").getPath(), "wms_dev@WONHIGH.COM");
        System.setProperty("java.security.auth.login.config", jaasConf.getAbsolutePath());*/
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        //System.setProperty("sun.security.krb5.debug","true");

    }
}
