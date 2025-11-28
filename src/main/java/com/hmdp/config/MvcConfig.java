package com.hmdp.config;

import com.hmdp.utils.LoginInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Component
public class MvcConfig implements WebMvcConfigurer {

    @Autowired
    private LoginInterceptor loginInterceptor;

    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(loginInterceptor)
                .excludePathPatterns("/user/code")
                .excludePathPatterns("/user/login")
                .excludePathPatterns("blog/hot")
                .excludePathPatterns("/shop/**")
                .excludePathPatterns("/shop-type/**")
                .excludePathPatterns("/upload/**")
                .excludePathPatterns("/voucher/**");
    }
}
