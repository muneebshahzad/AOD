$(document).ready(function () {
            // Load source data initially based on the default type
            var selectedType = $("#type").val();
            loadSourceData(selectedType);

            // Load received by data initially based on the default paymentVia
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
            var receivedBy = $("#receivedBy").val();

            console.log("Form Data:");
            console.log("Amount:", amount);
            console.log("Type:", type);
            console.log("Payment To:", source);
            console.log("Description:", description);
            console.log("Payment Via:", paymentVia);
            console.log("Payment by:", receivedBy);

            // Check if type and paymentVia are selected, if not, load default data
            if (!type) {
                type = "Profit Withdrawal"; // Set a default value or fetch from the server
                loadSourceData(type);
            }

            if (!paymentVia) {
                paymentVia = "Bank"; // Set a default value or fetch from the server
                loadReceivedByData(paymentVia);
            }

            // Fetch expense_id, payment_by, and payment_to using AJAX
            $.ajax({
                type: "POST",
                url: "/get_expense_ids",
                data: {
                    type: type,
                    paymentVia: paymentVia,
                    receivedBy: receivedBy
                },
                success: function (ids) {
                    console.log("IDs:");
                    console.log("Expense ID:", ids.expense_id);
                    console.log("Payment Via:", ids.payment_by);
                    console.log("Received By:", ids.payment_to);

                    // Prepare data for submission
                    var data = {
                        amount: amount,
                        type: ids.expense_id,
                        source: source,
                        description: description,
                        paymentVia: ids.payment_by,
                        receivedBy: ids.payment_to
                    };

                    // Submit form data to the server using AJAX
                    $.ajax({
                        type: "POST",
                        url: "/add_expense",
                        data: data,
                        success: function (response) {
                            console.log("Server Response:", response);
                            // Display success message and reset the form
                            displaySuccessMessage();
                            resetForm();
                        },
                        error: function (xhr, status, error) {
                            console.error("Error adding expense:", error);
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
                url: "/get_payment_to_data",
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
                var bankOption = ["Muneeb","Haider"]
                bankOption.forEach(function (option) {
                 var bankOptions = $("<option></option>").attr("value", option).text(option);
                 receivedByDropdown.append(bankOptions);
                });

            } else if (paymentVia === "Cash") {
                var cashOptions = ["Muneeb", "Haider", "Hamza", "Other"];
                cashOptions.forEach(function (option) {
                    var receivedByOption = $("<option></option>").attr("value", option).text(option);
                    receivedByDropdown.append(receivedByOption);
                });
            }
        }

        function displaySuccessMessage() {
            // Display a success message (You can customize this part)
            var successMessage = $("<div></div>")
                .addClass("alert alert-success")
                .text("Expense added successfully");
            $("#intro").prepend(successMessage);

            // Remove the success message after a few seconds
            setTimeout(function () {
                successMessage.remove();
            }, 3000);
        }

        function resetForm() {
            // Reset the form fields
            $("#amount").val("");
            $("#type").val("");
            $("#source").val("");
            $("#description").val("");
            $("#paymentVia").val("");
            $("#receivedBy").val("");
        }

        $("#amount").on("input", function () {
            // Allow only numbers and a dot (for decimal values)
            $(this).val($(this).val().replace(/[^0-9.]/g, ''));
        });

