      $(document).ready(function () {
    // Load source data initially based on the default type
    var selectedType = $("#type").val();
    loadSourceData(selectedType);

     // Load receivedby data initially based on the default type
    var selectedPaymentVia = $("#paymentVia").val();
    loadReceivedByData(selectedPaymentVia);


    $("#type").change(function () {
        var selectedType = $(this).val();
        loadSourceData(selectedType);
    });

    $("#paymentVia").change(function () {
        var selectedPaymentVia = $(this).val();
        loadReceivedByData(selectedPaymentVia);
    });
});

         function submitForm(event) {
         event.preventDefault();

        // Get form data
        var amount = $("#amount").val();
        var type = $("#type").val();
        var source = $("#source").val();
        var description = $("#description").val();
        var paymentVia = $("#paymentVia").val();
        var receivedBy = $("#receivedBy").val();  // Add this line to get receivedBy value

        console.log("Form Data:");
        console.log("Amount:", amount);
        console.log("Type:", type);
        console.log("Source:", source);
        console.log("Description:", description);
        console.log("Payment Via:", paymentVia);
        console.log("Received By:", receivedBy);

        // Check if type and paymentVia are selected, if not, load default data
        if (!type) {
            type = "Sales"; // Set a default value or fetch from the server
            loadSourceData(type);
        }

        if (!paymentVia) {
            paymentVia = "Bank"; // Set a default value or fetch from the server
            loadReceivedByData(paymentVia);
        }

        // Fetch income_id, payment_by, and payment_to using AJAX
        $.ajax({
            type: "POST",
            url: "/get_ids",
            data: {
                type: type,
                paymentVia: paymentVia,
                receivedBy: receivedBy  // Pass receivedBy value in the request
            },
            success: function (ids) {
                console.log("IDs:");
                console.log("Income ID:", ids.income_id);
                console.log("Payment By:", ids.payment_by);
                console.log("Payment To:", ids.payment_to);

                // Prepare data for submission
                var data = {
    amount: amount,
    type: ids.income_id,  // Change this to the correct key
    source: source,
    description: description,
    paymentVia: ids.payment_by,  // Change this to the correct key
    receivedBy: ids.payment_to  // Change this to the correct key
};

                // Submit form data to the server using AJAX
                $.ajax({
                    type: "POST",
                    url: "/add_income",
                    data: data,
                    success: function (response) {
                        console.log("Server Response:", response);
                        // Display success message and reset the form
                        displaySuccessMessage();
                        resetForm();
                    },
                    error: function (xhr, status, error) {
                        console.error("Error adding income:", error);
                    }
                });
            },
            error: function (xhr, status, error) {
                console.error("Error fetching IDs:", error);
            }
        });
    }

        function loadSourceData(type) {
            var sourceDropdown = $("#source");
            sourceDropdown.empty();

            // Fetch data from the server based on the selected type
            $.ajax({
                type: "POST",
                url: "/get_source_data",
                data: { type: type },
                success: function (data) {
                    data.forEach(function (item) {
                        var option = $("<option></option>").attr("value", item).text(item);
                        sourceDropdown.append(option);
                    });
                },
                error: function (xhr, status, error) {
                    console.error("Error fetching source data:", error);
                }
            });
        }

        function loadReceivedByData(paymentVia) {
            var receivedByDropdown = $("#receivedBy");
            receivedByDropdown.empty();

            if (paymentVia === "Bank") {
                var bankOption = $("<option></option>").attr("value", "Bank").text("Bank");
                receivedByDropdown.append(bankOption);
            } else if (paymentVia === "Cash") {
                var cashOptions = ["Muneeb", "Haider", "Hamza", "Other"];
                cashOptions.forEach(function (option) {
                    var receivedByOption = $("<option></option>").attr("value", option).text(option);
                    receivedByDropdown.append(receivedByOption);
                });
            }
        }

        function addIncome() {
    // Get form data
    var amount = $("#amount").val();
    var type = $("#type").val();
    var source = $("#source").val();
    var description = $("#description").val();
    var paymentVia = $("#paymentVia").val();
    var receivedBy = $("#receivedBy").val();

    console.log("Form Data:");
    console.log("Amount:", amount);
    console.log("Type:", type);
    console.log("Source:", source);
    console.log("Description:", description);
    console.log("Payment Via:", paymentVia);
    console.log("Received By:", receivedBy);

    // Check if type and paymentVia are selected, if not, load default data
    if (!type) {
        type = "Sales"; // Set a default value or fetch from the server
        loadSourceData(type);
    }

    if (!paymentVia) {
        paymentVia = "Bank"; // Set a default value or fetch from the server
        loadReceivedByData(paymentVia);
    }

    // Fetch income_id, payment_by, and payment_to using AJAX
    $.ajax({
        type: "POST",
        url: "/get_ids",
        data: {
            type: type,
            paymentVia: paymentVia,
            receivedBy: receivedBy
        },
        success: function (ids) {
            console.log("IDs:");
            console.log("Income ID:", ids.income_id);
            console.log("Payment By:", ids.payment_by);
            console.log("Payment To:", ids.payment_to);

            // Prepare data for submission
           var data = {
    amount: amount,
    income_id: ids.income_id,
    source: source,
    description: description,
    payment_by: ids.payment_by,
    payment_to: ids.payment_to
};

// Submit form data to the server using AJAX
$.ajax({
    type: "POST",
    url: "/add_income",
    data: data,
    success: function (response) {
        console.log("Server Response:", response);
        // Display success message and reset the form
        displaySuccessMessage();
        resetForm();
    },
    error: function (xhr, status, error) {
        console.error("Error adding income:", error);
    }
});
        },
        error: function (xhr, status, error) {
            console.error("Error fetching IDs:", error);
        }
    });
}

        function displaySuccessMessage() {
            // Display a success message (You can customize this part)
            var successMessage = $("<div></div>")
                .addClass("alert alert-success")
                .text("Submitted successfully");
            $("body").append(successMessage);

            // Remove the success message after a few seconds
            setTimeout(function () {
                successMessage.remove();
            }, 3000);
        }

        function resetForm() {
            // Reset the form fields
            $("#amount").val("");
            $("#type").val("");
            $("#source").empty(); // Assuming you want to clear the source dropdown
            $("#description").val("");
            $("#paymentVia").val("");
            $("#receivedBy").empty(); // Assuming you want to clear the receivedBy dropdown
        }
        $("#amount").on("input", function () {
    // Allow only numbers and a dot (for decimal values)
    $(this).val($(this).val().replace(/[^0-9.]/g, ''));
});

        function displaySuccessMessage() {
    // Display a success message (You can customize this part)
    var successMessage = $("<div></div>")
        .addClass("alert alert-success")
        .text("Submitted successfully");
    $("#intro").prepend(successMessage);

    // Remove the success message after a few seconds
    setTimeout(function () {
        successMessage.remove();
    }, 3000);
}