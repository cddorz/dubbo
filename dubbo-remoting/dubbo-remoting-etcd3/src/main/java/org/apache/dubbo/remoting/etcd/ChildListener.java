package org.apache.dubbo.remoting.etcd;

import java.util.List;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-17 19:19
 */
public interface ChildListener {

    void childChanged(String path, List<String> children);

}
