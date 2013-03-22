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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.channel.ChannelOption;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/*
 * Hooks for configuring various parts of Netty.
 */
public abstract class NettyConfigBuilderBase
{
    public static final String EMPTY_STRING = "";
    public static final String UNDERSCORE = "_";
    public static final String SOCKET_OPTION_PREFIX = "SO";
    private final Map<String, ChannelOption<?>> channelOptions;
    private final Map<String, ChannelOption<?>> socketPrefixedChannelOptions;
    private final Map<String, ChannelOption<?>> customChannelOptions;

    @Inject
    public NettyConfigBuilderBase()
    {
        channelOptions = findPrefixedChannelOptions("");
        socketPrefixedChannelOptions = findPrefixedChannelOptions(SOCKET_OPTION_PREFIX);
        customChannelOptions = ImmutableMap.<String, ChannelOption<?>>of(
                "setReceiveBufferSize", ChannelOption.SO_RCVBUF,
                "setSendBufferSize", ChannelOption.SO_SNDBUF,
                "setReuseAddress", ChannelOption.SO_REUSEADDR
        );
    }

    // Magic alert ! Content of this class is considered ugly and magical.
    // For all intents and purposes this is to create a Map with the correct
    // key and value pairs for Netty's Bootstrap to consume.
    //
    // sadly Netty does not define any constant strings whatsoever for the proper key to
    // use and it's all based on standard java bean attributes.
    //
    // A ChannelConfig impl in netty is also tied with a socket, but since all
    // these configs are interfaces we can do a bit of magic hacking here.

    protected class Magic implements InvocationHandler
    {
        private final Map<ChannelOption<?>, Object> options;

        public Magic()
        {
            this.options = newHashMap();
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable
        {
            if (args == null) {
                args = new Object[0];
            }

            // we are only interested in setters with single arg
            if (proxy != null) {
                if (method.getName().equals("toString") && args.length == 0) {
                    return "this is a magic proxy";
                }
                else if (method.getName().equals("equals") && args.length == 1) {
                    return Boolean.FALSE;
                }
                else if (method.getName().equals("hashCode") && args.length == 0) {
                    return 0;
                }
            }

            if (method.getName().equals("getOptions") && args.length == 0) {
                return this.options;
            }

            // we don't support multi-arg setters
            if (method.getName().startsWith("set") && args.length == 1) {
                ChannelOption<?> key = inferChannelOptionFromSetterMethodName(method.getName());
                Object value = args[0];

                if (key != null) {
                    options.put(key, value);
                    return proxy;
                }

                throw new Exception("Could not find ChannelOption corresponding to setter method: " +
                                    method.getName());
            }

            throw new UnsupportedOperationException("Only setters and getOptions are supported");
        }

        /**
         * Infer a {@link ChannelOption} from a setter method name
         *
         * @param setterMethodName
         * @return
         */
        private ChannelOption<?> inferChannelOptionFromSetterMethodName(String setterMethodName)
        {
            // Many of the options can be found just by removing the "set" prefix, and finding
            // a matching ChannelOption static field (ignoring case, and dropping underscores
            // from ChannelOption static field names).
            String lookupName = setterMethodName.substring(3).toUpperCase();
            if (channelOptions.containsKey(lookupName)) {
                return channelOptions.get(lookupName);
            }

            // Some more options can be found just by the above method, but adding "SO_" prefix
            String prefixedLookupName = SOCKET_OPTION_PREFIX + lookupName;
            if (socketPrefixedChannelOptions.containsKey(prefixedLookupName)) {
                return socketPrefixedChannelOptions.get(prefixedLookupName);
            }

            // And a few options cannot be found either of the above ways, so there is a custom
            // map of entries to handle these
            if (customChannelOptions.containsKey(setterMethodName)) {
                return customChannelOptions.get(setterMethodName);
            }

            return null;
        }
    }

    /**
     * Build an immutable {@link Map} to all the {@link ChannelOption} instances that start with a
     * given prefix
     *
     * @param prefix Only add {@link ChannelOption} with this prefix
     * @return The {@link Map} of {@link ChannelOption} instances
     */
    private Map<String, ChannelOption<?>> findPrefixedChannelOptions(String prefix)
    {
        if (!Strings.isNullOrEmpty(prefix.trim()) && prefix.endsWith(UNDERSCORE)) {
            prefix = prefix + UNDERSCORE;
        }

        ImmutableMap.Builder<String, ChannelOption<?>> builder = ImmutableMap.builder();

        for (Field optionField : ChannelOption.class.getFields()) {

            if (!ChannelOption.class.equals(optionField.getType())) {
                continue;
            }

            ChannelOption<?> option;
            try {
                option = (ChannelOption<?>) optionField.get(null);
            } catch (IllegalAccessException e) {
                continue;
            }

            String optionName = option.name();
            if (!optionName.startsWith(prefix)) {
                continue;
            }

            optionName = optionName.replace(UNDERSCORE, EMPTY_STRING);

            builder.put(optionName, option);
        }

        return builder.build();
    }
}
