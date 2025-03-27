package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/")
public class controller {

    @Autowired
    public UserRepository userRepository;

    @GetMapping("balance")
    public Balance getBalance(@RequestParam long userId){
        UserRecord userOpt = userRepository.findById(userId);
        if(userOpt == null) {
            return new Balance((float) 0);
        }else{

            return new Balance(userOpt.getBalance());
        }

    }

}
