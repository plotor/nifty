/*
 * Copyright (C) 2012-2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.nifty.core;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandler;

import java.lang.reflect.Method;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class NiftyChannelInitializers
{
    public static <C extends Channel> NiftyChannelInitializer<C> emptyChannelInitializer() {
        return new NiftyChannelInitializer<C>()
        {
            @Override
            public void initChannel(C channel) throws Exception
            {
            }
        };
    }

    @SafeVarargs
    public static <C extends Channel> NiftyChannelInitializer<C> composeChannelInitializers(
            final NiftyChannelInitializer<C>... initializers)
    {
        return new NiftyChannelInitializer<C>()
        {
            @Override
            public void initChannel(C channel) throws Exception
            {
                for (NiftyChannelInitializer<C> initializer : initializers) {
                    initializer.initChannel(channel);
                }
            }
        };
    }

    public static <C extends Channel> NiftyChannelInitializer<C> getOptionsInitializer(
            final Map<String, Object> bootstrapOptions)
    {
        return new NiftyChannelInitializer<C>()
        {
            @Override
            public void initChannel(C ch) throws Exception
            {
                applyBootstrapOptions(bootstrapOptions, ch);
            }
        };
    }

    private static void applyBootstrapOptions(Map<String, Object> options, Channel channel) throws Exception
    {
        ChannelConfig config = channel.config();
        for (Map.Entry<String, Object> option : options.entrySet()) {
            String optionName = option.getKey();
            final String setterMethodName = "set" + optionName.substring(0, 1).toUpperCase() + optionName.substring(1);
            Method method = findMethod(config.getClass(), setterMethodName);
            method.invoke(config, option.getValue());
        }
    }

    private static Method findMethod(Class configClass, String setterMethodName) throws NoSuchMethodException
    {
        checkNotNull(configClass);
        checkNotNull(setterMethodName);

        while (configClass != null && configClass != Object.class) {
            for (Method method : configClass.getDeclaredMethods()) {
                if (method.getName().equals(setterMethodName)) {
                    return method;
                }
            }
            configClass = configClass.getSuperclass();
        }

        throw new NoSuchMethodException(setterMethodName);
    }
}
